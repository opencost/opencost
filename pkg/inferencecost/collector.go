package inferencecost

import (
	"context"
	"fmt"
	"math"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/opencost/opencost/core/pkg/filter/allocation"
	"github.com/opencost/opencost/core/pkg/filter/ops"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/source"
	"gopkg.in/yaml.v2"
)

// AllocationQuerier is the subset of the cost model needed to fetch per-model
// infrastructure costs. Abstracted as an interface for testability.
type AllocationQuerier interface {
	// ComputeAllocation returns an AllocationSet for the given time window.
	ComputeAllocation(start, end time.Time) (*opencost.AllocationSet, error)
}

// Collector gathers per-model infrastructure costs from the OpenCost allocation
// layer and token/timing/cache metrics from the data source.
type Collector struct {
	allocationQuerier AllocationQuerier
	metricsQuerier    source.MetricsQuerier
	config            *Config
	unmatchedKeys     map[string]float64
	mu                sync.RWMutex
}

// NewCollector creates a Collector that uses the provided MetricsQuerier for
// inference metrics.
func NewCollector(config *Config, querier AllocationQuerier, metricsQuerier source.MetricsQuerier) (*Collector, error) {
	return &Collector{
		allocationQuerier: querier,
		metricsQuerier:    metricsQuerier,
		config:            config,
		unmatchedKeys:     make(map[string]float64),
	}, nil
}

// GetUnmatchedKeys returns the count of unmatched attribution keys per producer.
func (c *Collector) GetUnmatchedKeys() map[string]float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	res := make(map[string]float64, len(c.unmatchedKeys))
	for k, v := range c.unmatchedKeys {
		res[k] = v
	}
	return res
}

// CollectMetrics queries all data sources and returns one InferenceCost per
// model/namespace combination and the dimension tokens result.
func (c *Collector) CollectMetrics(ctx context.Context, start, end time.Time) ([]*InferenceCost, *source.InferenceDimensionResult, error) {
	// --- Infrastructure costs from OpenCost allocation layer ---
	allocationCosts, err := c.queryAllocationCosts(ctx, start, end)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query allocation costs: %w", err)
	}
	log.Infof("InferenceCost: collected allocation costs for %d model/namespace combinations", len(allocationCosts))

	// --- Token metrics from data source ---
	promptTokensFuture := c.metricsQuerier.QueryInferencePromptTokens(start, end)
	generationTokensFuture := c.metricsQuerier.QueryInferenceGenerationTokens(start, end)
	inputTimeFuture := c.metricsQuerier.QueryInferenceInputProcessingTime(start, end)
	outputTimeFuture := c.metricsQuerier.QueryInferenceOutputProcessingTime(start, end)
	cachedTokensFuture := c.metricsQuerier.QueryInferenceCachedTokens(start, end)
	cacheConfigFuture := c.metricsQuerier.QueryInferenceCacheConfig(end)
	dimTokensFuture := c.metricsQuerier.QueryInferenceDimensionTokens(start, end)

	// Await required metrics (prompt and generation tokens)
	promptTokensResults, err := promptTokensFuture.Await()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query prompt tokens: %w", err)
	}
	promptTokens := mergeTokenResults(promptTokensResults)

	generationTokensResults, err := generationTokensFuture.Await()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query generation tokens: %w", err)
	}
	generationTokens := mergeTokenResults(generationTokensResults)

	// --- Timing metrics (optional — degrade gracefully) ---
	inputProcessingTime := make(map[string]float64)
	if inputTimeResults, err := inputTimeFuture.Await(); err != nil {
		log.Warnf("InferenceCost: failed to query input processing time (will use multiplier fallback): %v", err)
	} else {
		inputProcessingTime = mergeProcessingTimeResults(inputTimeResults)
	}

	outputProcessingTime := make(map[string]float64)
	if outputTimeResults, err := outputTimeFuture.Await(); err != nil {
		log.Warnf("InferenceCost: failed to query output processing time (will use multiplier fallback): %v", err)
	} else {
		outputProcessingTime = mergeProcessingTimeResults(outputTimeResults)
	}

	// --- KV cache hits (optional — degrade gracefully) ---
	cachedTokens := make(map[string]float64)
	if cachedTokensResults, err := cachedTokensFuture.Await(); err != nil {
		log.Warnf("InferenceCost: failed to query KV cache hits (cacheSavingsFraction will be zero): %v", err)
	} else {
		cachedTokens = mergeTokenResults(cachedTokensResults)
	}

	// --- KV cache config (prefix caching enabled flag only) ---
	cacheConfigs := make(map[string]*cacheConfig)
	if cacheConfigResults, err := cacheConfigFuture.Await(); err != nil {
		log.Warnf("InferenceCost: failed to query cache config (prefix_caching_off detection disabled): %v", err)
	} else {
		cacheConfigs = mergeCacheConfigResults(cacheConfigResults)
	}

	// --- Attribution dimension tokens ---
	var dimResult *source.InferenceDimensionResult
	if dimTokensResults, err := dimTokensFuture.Await(); err != nil {
		log.Warnf("InferenceCost: failed to query dimension tokens: %v", err)
		dimResult = &source.InferenceDimensionResult{Values: make(map[source.InferenceDimensionKey]*source.InferenceDimensionTokens)}
	} else {
		dimResult = mergeDimensionResults(dimTokensResults)
	}

	modelCosts := c.combineMetrics(allocationCosts, promptTokens, generationTokens,
		inputProcessingTime, outputProcessingTime, cachedTokens, cacheConfigs, start, end)

	return modelCosts, dimResult, nil
}

// cacheConfig holds per-model KV cache configuration from vllm:cache_config_info.
type cacheConfig struct {
	prefixCachingEnabled bool
}

// allocationResult holds the two cost figures derived from one Allocation.
type allocationResult struct {
	allocationTotalCost float64
	usageTotalCost      float64
	namespace           string
	cluster             string
	pod                 string
	controller          string
	controllerKind      string
	container           string
}

// queryAllocationCosts calls the OpenCost allocation layer twice:
// once with idle sharing (for allocation costs) and once without (for usage costs).
// This ensures allocation costs reconcile to the bill while usage costs reflect
// only active compute without idle or waste.
// This approach was chosen rather than doing a single call and deducting idle and optionally shared, so that
// core logic is not duplicated.  A performance penalty is paid though.
func (c *Collector) queryAllocationCosts(ctx context.Context, start, end time.Time) (map[string]*allocationResult, error) {
	// Query 1: Allocation costs with idle sharing (reconciles to bill)
	allocationCosts, err := c.queryAllocationCostsWithIdle(ctx, start, end)
	if err != nil {
		return nil, fmt.Errorf("failed to query allocation costs with idle: %w", err)
	}

	// Query 2: Usage costs without idle sharing (active compute only)
	usageCosts, err := c.queryAllocationCostsWithoutIdle(ctx, start, end)
	if err != nil {
		return nil, fmt.Errorf("failed to query usage costs without idle: %w", err)
	}

	// Merge results: allocation costs from first query, usage costs from second
	results := make(map[string]*allocationResult)
	for key, allocResult := range allocationCosts {
		if allocResult == nil {
			continue
		}
		// Copy the full allocationResult so we retain pod/controller/container metadata
		copied := *allocResult
		copied.usageTotalCost = 0 // Will be filled from usageCosts
		results[key] = &copied
	}

	// Fill in usage costs from the second query
	for key, usageResult := range usageCosts {
		if result, exists := results[key]; exists {
			result.usageTotalCost = usageResult.usageTotalCost
		} else {
			// Model exists in usage query but not allocation query (shouldn't happen)
			log.Warnf("InferenceCost: model %s has usage cost but no allocation cost", key)
			results[key] = usageResult
		}
	}

	// Log the differences
	for key, result := range results {
		modelName, namespace := parseKey(key)
		if result.allocationTotalCost > 0 {
			log.Debugf("InferenceCost: model=%s ns=%s alloc=$%.4f usage=$%.4f (%.1f%% of alloc)",
				modelName, namespace, result.allocationTotalCost, result.usageTotalCost,
				(result.usageTotalCost/result.allocationTotalCost)*100)
		}
	}
	return results, nil
}

// queryAllocationCostsWithIdle queries allocations with idle sharing enabled.
func (c *Collector) queryAllocationCostsWithIdle(ctx context.Context, start, end time.Time) (map[string]*allocationResult, error) {
	as, err := c.allocationQuerier.ComputeAllocation(start, end)
	if err != nil {
		return nil, err
	}

	// Create a filter to match shared infrastructure allocations by label
	// This ensures allocations with the shared infra label are moved to shareSet
	// and distributed among other allocations, rather than aggregating into __unallocated__
	shareFilter := ops.Eq(
		ops.WithKey(allocation.FieldLabel, c.config.SharedInfraLabel),
		c.config.SharedInfraLabelValue,
	)

	opts := &opencost.AllocationAggregationOptions{
		ShareIdle:    opencost.ShareWeighted,
		ShareSplit:   opencost.ShareWeighted,
		Share:        shareFilter,
		SharedLabels: map[string][]string{c.config.SharedInfraLabel: {c.config.SharedInfraLabelValue}},
	}

	aggregateBy := []string{"label:" + c.config.ModelLabel}
	if err := as.AggregateBy(aggregateBy, opts); err != nil {
		return nil, fmt.Errorf("AggregateBy label:%s: %w", c.config.ModelLabel, err)
	}

	return c.extractAllocationResults(as, true)
}

// queryAllocationCostsWithoutIdle queries allocations without idle or shared
// infrastructure cost sharing. Usage costs reflect active compute only.
func (c *Collector) queryAllocationCostsWithoutIdle(ctx context.Context, start, end time.Time) (map[string]*allocationResult, error) {
	as, err := c.allocationQuerier.ComputeAllocation(start, end)
	if err != nil {
		return nil, err
	}

	// Create a filter to match shared infrastructure allocations by label
	// Even though we're not sharing costs (ShareSplit: ShareNone), we still need
	// the Share filter to identify and separate shared infra allocations from
	// regular allocations, preventing them from aggregating into __unallocated__
	shareFilter := ops.Eq(
		ops.WithKey(allocation.FieldLabel, c.config.SharedInfraLabel),
		c.config.SharedInfraLabelValue,
	)

	opts := &opencost.AllocationAggregationOptions{
		ShareIdle:    opencost.ShareNone,
		ShareSplit:   opencost.ShareNone,
		Share:        shareFilter,
		SharedLabels: map[string][]string{c.config.SharedInfraLabel: {c.config.SharedInfraLabelValue}},
	}

	aggregateBy := []string{"label:" + c.config.ModelLabel}
	if err := as.AggregateBy(aggregateBy, opts); err != nil {
		return nil, fmt.Errorf("AggregateBy label:%s: %w", c.config.ModelLabel, err)
	}

	return c.extractAllocationResults(as, false)
}

// extractAllocationResults extracts cost data from an AllocationSet.
func (c *Collector) extractAllocationResults(as *opencost.AllocationSet, isAllocationCost bool) (map[string]*allocationResult, error) {
	results := make(map[string]*allocationResult)
	for name, alloc := range as.Allocations {
		if alloc == nil {
			continue
		}
		// Skip the synthetic __idle__ and __unallocated__ entries.
		if strings.HasPrefix(name, "__") {
			continue
		}

		modelName := extractModelName(alloc, c.config.ModelLabel)
		if modelName == "" {
			continue
		}

		namespace := ""
		cluster := ""
		pod := ""
		controller := ""
		controllerKind := ""
		container := ""

		if alloc.Properties != nil {
			namespace = alloc.Properties.Namespace
			cluster = alloc.Properties.Cluster
			pod = alloc.Properties.Pod
			controller = alloc.Properties.Controller
			controllerKind = alloc.Properties.ControllerKind
			container = alloc.Properties.Container
		}

		key := modelNamespaceKey(modelName, namespace)

		// Accumulate costs for the same model/namespace key
		existing, exists := results[key]
		if !exists {
			existing = &allocationResult{
				namespace:      namespace,
				cluster:        cluster,
				pod:            pod,
				controller:     controller,
				controllerKind: controllerKind,
				container:      container,
			}
			results[key] = existing
		}

		if isAllocationCost {
			// For allocation cost: use TotalCost() which includes idle and shared
			existing.allocationTotalCost += alloc.TotalCost()
		} else {
			// For usage cost: use TotalCost() from the ShareNone query (no idle),
			// then scale GPU, CPU, and RAM by their actual utilisation when available.
			// This ensures costBasis=usage reflects actual resource consumption rather
			// than the full reservation cost.
			//
			// Resources intentionally left unscaled:
			//   Network  — already billed by actual bytes transferred, no reservation to remove.
			//   PV       — billed by provisioned capacity; no IO utilisation metric available.
			//   LB       — billed per hour of existence; no per-request utilisation signal.
			//   External — opaque cloud billing pass-through; no usage signal attached.
			cost := alloc.TotalCost()

			// GPU: scale by SM duty cycle (GPUUsageAverage ∈ [0,1]).
			// GPUHours (and therefore GPUCost) always reflects the full reservation;
			// GPUUsageAverage is the fraction of time the GPU cores were active.
			// Any non-nil value is clamped to [0,1] so that zero utilisation
			// correctly reduces cost to $0 and out-of-range values are handled
			// deterministically rather than silently ignored.
			if alloc.GPUAllocation != nil && alloc.GPUAllocation.GPUUsageAverage != nil {
				util := *alloc.GPUAllocation.GPUUsageAverage
				if util < 0 {
					util = 0
				} else if util > 1 {
					util = 1
				}
				scaledGPUCost := alloc.GPUTotalCost() * util
				cost = cost - alloc.GPUTotalCost() + scaledGPUCost
				log.Debugf("InferenceCost usage: GPU scaled model=%s ns=%s orig=$%.4f scaled=$%.4f util=%.1f%%",
					modelName, namespace, alloc.GPUTotalCost(), scaledGPUCost, util*100)
			}

			// CPU: scale by core utilisation ratio (usage / request).
			// CPUCoreRequestAverage and CPUCoreUsageAverage are plain float64 (not pointers).
			if alloc.CPUCoreRequestAverage > 0 &&
				alloc.CPUCoreUsageAverage > 0 &&
				alloc.CPUCoreUsageAverage < alloc.CPUCoreRequestAverage {
				cpuUtil := alloc.CPUCoreUsageAverage / alloc.CPUCoreRequestAverage
				scaledCPUCost := alloc.CPUTotalCost() * cpuUtil
				cost = cost - alloc.CPUTotalCost() + scaledCPUCost
				log.Debugf("InferenceCost usage: CPU scaled model=%s ns=%s orig=$%.4f scaled=$%.4f util=%.1f%%",
					modelName, namespace, alloc.CPUTotalCost(), scaledCPUCost, cpuUtil*100)
			}

			// RAM: scale by byte utilisation ratio (usage / request).
			// RAMBytesRequestAverage and RAMBytesUsageAverage are plain float64 (not pointers).
			if alloc.RAMBytesRequestAverage > 0 &&
				alloc.RAMBytesUsageAverage > 0 &&
				alloc.RAMBytesUsageAverage < alloc.RAMBytesRequestAverage {
				ramUtil := alloc.RAMBytesUsageAverage / alloc.RAMBytesRequestAverage
				scaledRAMCost := alloc.RAMTotalCost() * ramUtil
				cost = cost - alloc.RAMTotalCost() + scaledRAMCost
				log.Debugf("InferenceCost usage: RAM scaled model=%s ns=%s orig=$%.4f scaled=$%.4f util=%.1f%%",
					modelName, namespace, alloc.RAMTotalCost(), scaledRAMCost, ramUtil*100)
			}

			existing.usageTotalCost += cost
		}

		// When aggregating multiple allocations, preserve the first non-empty values
		// for pod, controller, and container. This provides representative values
		// when costs are aggregated across multiple pods/containers.
		if existing.pod == "" && pod != "" {
			existing.pod = pod
		}
		if existing.controller == "" && controller != "" {
			existing.controller = controller
		}
		if existing.controllerKind == "" && controllerKind != "" {
			existing.controllerKind = controllerKind
		}
		if existing.container == "" && container != "" {
			existing.container = container
		}
	}

	return results, nil
}

// extractModelName extracts the model name from the allocation name or label.
// After AggregateBy("label:<key>"), the allocation Name is the label value.
func extractModelName(alloc *opencost.Allocation, _ string) string {
	if alloc == nil {
		return ""
	}
	// AggregateBy sets the Name to the label value.
	return alloc.Name
}

// canonicalModelName normalizes a model name by stripping any org/vendor prefix
// before the last "/".
// Examples:
//   - "MiniMaxAI/MiniMax-M2.7" -> "MiniMax-M2.7"
//   - "google/gemma-4-31B" -> "gemma-4-31B"
func canonicalModelName(modelName string) string {
	if idx := strings.LastIndex(modelName, "/"); idx >= 0 {
		return modelName[idx+1:]
	}
	return modelName
}

// reconcileTokenKeys re-keys entries only when there is a confirmed mismatch
// between the metric key and the allocation-backed model key for the same
// namespace.
//
// Two common mismatch examples:
//  1. Fully-qualified vLLM model name vs short allocation label:
//     "google/gemma-4-31B:llm-d-pic" -> "gemma-4-31B:llm-d-pic"
//  2. Fully-qualified vLLM model name vs short allocation label with a
//     different vendor/org prefix:
//     "MiniMaxAI/MiniMax-M2.7:llm-d-pic" -> "MiniMax-M2.7:llm-d-pic"
//
// Exact matches are preserved. Keys with no matching allocation-backed target
// are also preserved unchanged. A warning is logged for every remapped key so
// the mismatch is auditable.
//
// Returns both the reconciled map and a set of keys that were remapped (to be
// excluded later).
func reconcileTokenKeys(tokens map[string]float64, allocCosts map[string]*allocationResult) (map[string]float64, map[string]struct{}) {
	// Build index: normalizedShortName:namespace -> allocKey, preferring
	// allocation keys that are already in short-name form.
	shortIndex := make(map[string]string, len(allocCosts))
	for allocKey := range allocCosts {
		modelName, namespace := parseKey(allocKey)
		shortName := canonicalModelName(modelName)
		shortKey := modelNamespaceKey(shortName, namespace)

		if existing, found := shortIndex[shortKey]; found {
			existingModelName, _ := parseKey(existing)
			if existingModelName == shortName {
				continue
			}
		}
		shortIndex[shortKey] = allocKey
	}

	out := make(map[string]float64, len(tokens))
	remappedKeys := make(map[string]struct{})

	for k, v := range tokens {
		modelName, namespace := parseKey(k)
		shortName := canonicalModelName(modelName)
		shortKey := modelNamespaceKey(shortName, namespace)

		if allocKey, found := shortIndex[shortKey]; found {
			if k != allocKey {
				log.Warnf("InferenceCost: remapping metric key %q → %q (model-name mismatch with allocation label)", k, allocKey)
				out[allocKey] += v
				remappedKeys[k] = struct{}{}
				continue
			}
		}

		out[k] = v
	}
	return out, remappedKeys
}

// reconcileCacheConfigKeys re-keys a cacheConfig map the same way reconcileTokenKeys
// does for float64 maps — handling fully-qualified vs short model name mismatches.
func reconcileCacheConfigKeys(configs map[string]*cacheConfig, allocCosts map[string]*allocationResult) (map[string]*cacheConfig, map[string]struct{}) {
	shortIndex := make(map[string]string, len(allocCosts))
	for allocKey := range allocCosts {
		modelName, namespace := parseKey(allocKey)
		shortName := canonicalModelName(modelName)
		shortKey := modelNamespaceKey(shortName, namespace)
		if _, exists := shortIndex[shortKey]; !exists {
			shortIndex[shortKey] = allocKey
		}
	}

	out := make(map[string]*cacheConfig, len(configs))
	remappedKeys := make(map[string]struct{})

	for k, v := range configs {
		modelName, namespace := parseKey(k)
		shortName := canonicalModelName(modelName)
		shortKey := modelNamespaceKey(shortName, namespace)

		if allocKey, found := shortIndex[shortKey]; found {
			if k != allocKey {
				log.Warnf("InferenceCost: remapping cache config key %q → %q (model-name mismatch with allocation label)", k, allocKey)
				out[allocKey] = v
				remappedKeys[k] = struct{}{}
				continue
			}
		}

		out[k] = v
	}
	return out, remappedKeys
}

// combineMetrics joins all data sources into InferenceCost structs.
func (c *Collector) combineMetrics(
	allocCosts map[string]*allocationResult,
	promptTokens, generationTokens,
	inputProcessingTime, outputProcessingTime,
	cachedTokens map[string]float64,
	cacheConfigs map[string]*cacheConfig,
	start, end time.Time,
) []*InferenceCost {

	// Reconcile token map keys against allocation keys to handle the case where
	// vLLM reports a fully-qualified model name (e.g. "org/model") but the K8s
	// pod label uses only the short name ("model"). Re-keying fires only when a
	// mismatch is detected; keys that already match are left unchanged.
	// Track which keys were remapped so we can exclude them from final results.
	var remappedKeys map[string]struct{}
	promptTokens, remappedKeys = reconcileTokenKeys(promptTokens, allocCosts)

	var remapped map[string]struct{}
	generationTokens, remapped = reconcileTokenKeys(generationTokens, allocCosts)
	for k := range remapped {
		remappedKeys[k] = struct{}{}
	}

	inputProcessingTime, remapped = reconcileTokenKeys(inputProcessingTime, allocCosts)
	for k := range remapped {
		remappedKeys[k] = struct{}{}
	}

	outputProcessingTime, remapped = reconcileTokenKeys(outputProcessingTime, allocCosts)
	for k := range remapped {
		remappedKeys[k] = struct{}{}
	}

	cachedTokens, remapped = reconcileTokenKeys(cachedTokens, allocCosts)
	for k := range remapped {
		remappedKeys[k] = struct{}{}
	}

	cacheConfigs, remapped = reconcileCacheConfigKeys(cacheConfigs, allocCosts)
	for k := range remapped {
		remappedKeys[k] = struct{}{}
	}

	// Union of all keys across sources.
	// Include timing/cache maps as well so models that only appear in those
	// sources are not dropped before cost calculation.
	keys := make(map[string]struct{})
	for k := range allocCosts {
		keys[k] = struct{}{}
	}
	for k := range promptTokens {
		keys[k] = struct{}{}
	}
	for k := range generationTokens {
		keys[k] = struct{}{}
	}
	for k := range inputProcessingTime {
		keys[k] = struct{}{}
	}
	for k := range outputProcessingTime {
		keys[k] = struct{}{}
	}
	for k := range cachedTokens {
		keys[k] = struct{}{}
	}
	for k := range cacheConfigs {
		keys[k] = struct{}{}
	}

	results := make([]*InferenceCost, 0, len(keys))
	for key := range keys {
		// Skip keys that were remapped to avoid duplicate series
		if _, wasRemapped := remappedKeys[key]; wasRemapped {
			continue
		}

		modelName, namespace := parseKey(key)

		cfg := cacheConfigs[key]
		var prefixCachingEnabled, cacheConfigKnown bool
		if cfg != nil {
			prefixCachingEnabled = cfg.prefixCachingEnabled
			cacheConfigKnown = true
		}

		ic := &InferenceCost{
			Properties: InferenceCostProperties{
				ModelName:    modelName,
				Namespace:    namespace,
				WorkloadType: "inference",
			},
			PromptTokens:         promptTokens[key],
			GenerationTokens:     generationTokens[key],
			InputProcessingTime:  inputProcessingTime[key],
			OutputProcessingTime: outputProcessingTime[key],
			CachedTokens:         cachedTokens[key],
			PrefixCachingEnabled: prefixCachingEnabled,
			CacheConfigKnown:     cacheConfigKnown,
			Timestamp:            end,
		}
		ic.Window.Start = start
		ic.Window.End = end

		if ar, ok := allocCosts[key]; ok {
			ic.AllocationTotalCost = ar.allocationTotalCost
			ic.UsageTotalCost = ar.usageTotalCost
			ic.Properties.Cluster = ar.cluster
			ic.Properties.Pod = ar.pod
			ic.Properties.Controller = ar.controller
			ic.Properties.ControllerKind = ar.controllerKind
			ic.Properties.Container = ar.container
			if namespace == "" {
				ic.Properties.Namespace = ar.namespace
			}
		}

		ic.TotalTokens = ic.PromptTokens + ic.GenerationTokens
		ic.EffectiveInputTokens = ic.PromptTokens - ic.CachedTokens
		if ic.EffectiveInputTokens < 0 {
			ic.EffectiveInputTokens = 0
		}

		results = append(results, ic)
	}
	return results
}

func modelNamespaceKey(modelName, namespace string) string {
	return modelName + ":" + namespace
}

func parseKey(key string) (modelName, namespace string) {
	idx := strings.IndexByte(key, ':')
	if idx < 0 {
		return key, "unknown"
	}
	return key[:idx], key[idx+1:]
}

// mergeTokenResults merges multiple InferenceTokensResult into a single map
func mergeTokenResults(results []*source.InferenceTokensResult) map[string]float64 {
	merged := make(map[string]float64)
	for _, result := range results {
		for k, v := range result.Values {
			merged[k] = v
		}
	}
	return merged
}

// mergeProcessingTimeResults merges multiple InferenceProcessingTimeResult into a single map
func mergeProcessingTimeResults(results []*source.InferenceProcessingTimeResult) map[string]float64 {
	merged := make(map[string]float64)
	for _, result := range results {
		for k, v := range result.Values {
			merged[k] = v
		}
	}
	return merged
}

// mergeCacheConfigResults merges multiple InferenceCacheConfigResult into a single map
func mergeCacheConfigResults(results []*source.InferenceCacheConfigResult) map[string]*cacheConfig {
	merged := make(map[string]*cacheConfig)
	for _, result := range results {
		for k, v := range result.Configs {
			merged[k] = &cacheConfig{prefixCachingEnabled: v.PrefixCachingEnabled}
		}
	}
	return merged
}

// mergeDimensionResults merges multiple InferenceDimensionResult into a single result
func mergeDimensionResults(results []*source.InferenceDimensionResult) *source.InferenceDimensionResult {
	out := &source.InferenceDimensionResult{
		Values: make(map[source.InferenceDimensionKey]*source.InferenceDimensionTokens),
	}
	for _, res := range results {
		if res == nil {
			continue
		}
		for k, v := range res.Values {
			if v == nil {
				continue
			}
			entry, ok := out.Values[k]
			if !ok {
				entry = &source.InferenceDimensionTokens{}
				out.Values[k] = entry
			}
			entry.PromptTokens += v.PromptTokens
			entry.CompletionTokens += v.CompletionTokens
			entry.EstimatedPromptTokens += v.EstimatedPromptTokens
			entry.EstimatedCompletionTokens += v.EstimatedCompletionTokens
		}
	}
	return out
}

func (c *Collector) loadProducerMappings() []*ProducerMapping {
	if c.config.AttributionConfigPath != "" {
		data, err := os.ReadFile(c.config.AttributionConfigPath)
		if err != nil {
			log.Errorf("InferenceCost: failed to read attribution config at %s: %v", c.config.AttributionConfigPath, err)
			return nil
		}
		var cfg struct {
			Producers []ProducerMapping `yaml:"producers"`
		}
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			log.Errorf("InferenceCost: failed to parse attribution config YAML: %v", err)
			return nil
		}
		var mappings []*ProducerMapping
		for i := range cfg.Producers {
			p := cfg.Producers[i]
			if p.Name == "" {
				p.Name = "default"
			}
			if p.TenantLabel == "" {
				p.TenantLabel = "tenant_id"
			}
			if p.ModelLabel == "" {
				p.ModelLabel = "serving_model"
			}
			if p.NamespaceLabel == "" {
				p.NamespaceLabel = "deployment_namespace"
			}
			if p.IdentitySentinel == "" {
				p.IdentitySentinel = "unattributed"
			}
			mappings = append(mappings, &p)
		}
		return mappings
	}

	p := &ProducerMapping{
		Name:                        "default",
		InputTokensSeries:           c.config.AttributionInputTokensSeries,
		OutputTokensSeries:          c.config.AttributionOutputTokensSeries,
		InputTokensEstimatedSeries:  c.config.AttributionInputTokensEstSeries,
		OutputTokensEstimatedSeries: c.config.AttributionOutputTokensEstSeries,
		TenantLabel:                 c.config.AttributionTenantLabel,
		WorkloadLabel:               c.config.AttributionWorkloadLabel,
		UserLabel:                   c.config.AttributionUserLabel,
		ModelLabel:                  c.config.AttributionModelLabel,
		NamespaceLabel:              c.config.AttributionNamespaceLabel,
		IdentitySentinel:            c.config.AttributionIdentitySentinel,
		UserSentinel:                c.config.AttributionUserSentinel,
	}
	if p.TenantLabel == "" {
		p.TenantLabel = "tenant_id"
	}
	if p.ModelLabel == "" {
		p.ModelLabel = "serving_model"
	}
	if p.NamespaceLabel == "" {
		p.NamespaceLabel = "deployment_namespace"
	}
	if p.IdentitySentinel == "" {
		p.IdentitySentinel = "unattributed"
	}
	return []*ProducerMapping{p}
}

func parseNamespaceMap(s string) map[string]string {
	res := make(map[string]string)
	if s == "" {
		return res
	}
	pairs := strings.Split(s, ";")
	for _, pair := range pairs {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) == 2 {
			res[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
		}
	}
	return res
}

func isModelInScope(model, namespace string, scope ProducerScope) bool {
	if len(scope.ExcludeServingModels) > 0 {
		for _, m := range scope.ExcludeServingModels {
			if m == model {
				return false
			}
		}
	}
	if len(scope.ServingModels) > 0 {
		matched := false
		for _, m := range scope.ServingModels {
			if m == model {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	if len(scope.Namespaces) > 0 && namespace != "" {
		matched := false
		for _, ns := range scope.Namespaces {
			if ns == namespace {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

func shortModelName(model string) string {
	idx := strings.LastIndexByte(model, '/')
	if idx >= 0 {
		return model[idx+1:]
	}
	return model
}

// BuildDimensionCosts attributes model infrastructure costs to tenant/workload/user dimensions.
func (c *Collector) BuildDimensionCosts(modelCosts []*InferenceCost, dimTokens *source.InferenceDimensionResult) []*InferenceCost {
	if len(modelCosts) == 0 {
		return nil
	}
	if dimTokens == nil {
		dimTokens = &source.InferenceDimensionResult{
			Values: make(map[source.InferenceDimensionKey]*source.InferenceDimensionTokens),
		}
	}

	mappings := c.loadProducerMappings()
	mappingByName := make(map[string]*ProducerMapping, len(mappings))
	for _, m := range mappings {
		mappingByName[m.Name] = m
	}

	modelCostMap := make(map[string]*InferenceCost, len(modelCosts))
	modelToCostSlice := make(map[string][]*InferenceCost)
	for _, mc := range modelCosts {
		key := modelNamespaceKey(mc.Properties.ModelName, mc.Properties.Namespace)
		modelCostMap[key] = mc
		modelToCostSlice[mc.Properties.ModelName] = append(modelToCostSlice[mc.Properties.ModelName], mc)
		short := canonicalModelName(mc.Properties.ModelName)
		if short != mc.Properties.ModelName {
			modelToCostSlice[short] = append(modelToCostSlice[short], mc)
		}
	}

	findMatchingModelCost := func(servingModel, namespace string) *InferenceCost {
		key := modelNamespaceKey(servingModel, namespace)
		if mc, ok := modelCostMap[key]; ok {
			return mc
		}
		short := canonicalModelName(servingModel)
		if mc, ok := modelCostMap[modelNamespaceKey(short, namespace)]; ok {
			return mc
		}
		for _, mc := range modelCosts {
			if (mc.Properties.ModelName == servingModel || canonicalModelName(mc.Properties.ModelName) == short) && mc.Properties.Namespace == namespace {
				return mc
			}
		}
		return nil
	}

	nsMap := parseNamespaceMap(c.config.AttributionNamespaceMap)

	inScopeCountByProducer := make(map[string]int)
	matchedByProducer := make(map[string]int)
	unmatchedByProducer := make(map[string]float64)
	producersByDeployment := make(map[string]map[string]bool)

	type resolvedEntry struct {
		key         source.InferenceDimensionKey
		tokens      source.InferenceDimensionTokens
		approximate bool
	}
	var entries []resolvedEntry

	for dimKey, tokens := range dimTokens.Values {
		mapping := mappingByName[dimKey.Producer]
		var scope ProducerScope
		identitySentinel := "unattributed"
		userSentinel := ""
		workloadLabelConfigured := true
		if mapping != nil {
			scope = mapping.Scope
			identitySentinel = mapping.IdentitySentinel
			userSentinel = mapping.UserSentinel
			if mapping.WorkloadLabel == "" {
				workloadLabelConfigured = false
			}
		}

		// Scope filtering
		if !isModelInScope(dimKey.ServingModel, dimKey.Namespace, scope) {
			continue
		}

		inScopeCountByProducer[dimKey.Producer]++
		if dimKey.Namespace != "" {
			depKey := modelNamespaceKey(dimKey.ServingModel, dimKey.Namespace)
			if producersByDeployment[depKey] == nil {
				producersByDeployment[depKey] = make(map[string]bool)
			}
			producersByDeployment[depKey][dimKey.Producer] = true
		}

		// Missing identity normalization
		tenantID := dimKey.TenantID
		if tenantID == "" || (identitySentinel != "" && tenantID == identitySentinel) {
			tenantID = "__unidentified__"
		}
		workloadID := dimKey.WorkloadID
		if workloadLabelConfigured {
			if workloadID == "" || (identitySentinel != "" && workloadID == identitySentinel) {
				workloadID = "__unidentified__"
			}
		} else {
			workloadID = ""
		}
		userID := dimKey.UserID
		if userSentinel != "" && userID == userSentinel {
			userID = ""
		}

		// Missing namespace resolution
		ns := dimKey.Namespace
		if ns == "" {
			candidates := modelToCostSlice[dimKey.ServingModel]
			if len(candidates) == 0 {
				candidates = modelToCostSlice[canonicalModelName(dimKey.ServingModel)]
			}
			uniqueNamespaces := make(map[string]bool)
			for _, mc := range candidates {
				uniqueNamespaces[mc.Properties.Namespace] = true
			}
			if len(uniqueNamespaces) == 1 {
				for uNs := range uniqueNamespaces {
					ns = uNs
				}
			} else if len(uniqueNamespaces) > 1 {
				if mappedNs, ok := nsMap[dimKey.ServingModel]; ok && mappedNs != "" {
					ns = mappedNs
				} else if mappedNs, ok := nsMap[canonicalModelName(dimKey.ServingModel)]; ok && mappedNs != "" {
					ns = mappedNs
				} else {
					totalEngineTokens := 0.0
					for _, mc := range candidates {
						totalEngineTokens += (mc.PromptTokens + mc.GenerationTokens)
					}
					if totalEngineTokens > 0 {
						for _, mc := range candidates {
							engTokens := mc.PromptTokens + mc.GenerationTokens
							if engTokens > 0 {
								share := engTokens / totalEngineTokens
								splitTokens := source.InferenceDimensionTokens{
									PromptTokens:              tokens.PromptTokens * share,
									CompletionTokens:          tokens.CompletionTokens * share,
									EstimatedPromptTokens:     tokens.EstimatedPromptTokens * share,
									EstimatedCompletionTokens: tokens.EstimatedCompletionTokens * share,
								}
								splitKey := source.InferenceDimensionKey{
									Producer:     dimKey.Producer,
									UserID:       userID,
									TenantID:     tenantID,
									WorkloadID:   workloadID,
									ServingModel: dimKey.ServingModel,
									Namespace:    mc.Properties.Namespace,
								}
								entries = append(entries, resolvedEntry{key: splitKey, tokens: splitTokens, approximate: true})
							}
						}
						continue
					}
				}
			}
		}

		splitKey := source.InferenceDimensionKey{
			Producer:     dimKey.Producer,
			UserID:       userID,
			TenantID:     tenantID,
			WorkloadID:   workloadID,
			ServingModel: dimKey.ServingModel,
			Namespace:    ns,
		}
		entries = append(entries, resolvedEntry{key: splitKey, tokens: *tokens, approximate: false})
	}

	for _, entry := range entries {
		matched := findMatchingModelCost(entry.key.ServingModel, entry.key.Namespace)
		if matched == nil {
			unmatchedByProducer[entry.key.Producer]++
			log.Warnf("InferenceCost: unmatched attribution key (producer=%s, model=%s, ns=%s)", entry.key.Producer, entry.key.ServingModel, entry.key.Namespace)
		} else {
			matchedByProducer[entry.key.Producer]++
		}
	}

	c.mu.Lock()
	c.unmatchedKeys = unmatchedByProducer
	c.mu.Unlock()

	for _, mapping := range mappings {
		prod := mapping.Name
		if inScopeCountByProducer[prod] > 0 && matchedByProducer[prod] == 0 {
			log.Errorf("InferenceCost: attribution health check failed for producer %q with namespace label %q: none of its %d keys joined a model entry", prod, mapping.NamespaceLabel, inScopeCountByProducer[prod])
		}
	}

	for dep, prods := range producersByDeployment {
		if len(prods) > 1 {
			var prodList []string
			for p := range prods {
				prodList = append(prodList, p)
			}
			log.Warnf("InferenceCost: overlapping producers %v detected for deployment %s without scope separation; potential double-counting", prodList, dep)
		}
	}

	type identityKey struct {
		UserID       string
		TenantID     string
		WorkloadID   string
		ServingModel string
		Namespace    string
	}
	type identityAcc struct {
		PromptTokens              float64
		GenerationTokens          float64
		EstimatedPromptTokens     float64
		EstimatedCompletionTokens float64
		Approximate               bool
	}

	merged := make(map[identityKey]*identityAcc)
	for _, entry := range entries {
		matched := findMatchingModelCost(entry.key.ServingModel, entry.key.Namespace)
		if matched == nil {
			continue
		}
		idk := identityKey{
			UserID:       entry.key.UserID,
			TenantID:     entry.key.TenantID,
			WorkloadID:   entry.key.WorkloadID,
			ServingModel: matched.Properties.ModelName,
			Namespace:    matched.Properties.Namespace,
		}
		acc, ok := merged[idk]
		if !ok {
			acc = &identityAcc{}
			merged[idk] = acc
		}
		acc.PromptTokens += entry.tokens.PromptTokens
		acc.GenerationTokens += entry.tokens.CompletionTokens
		acc.EstimatedPromptTokens += entry.tokens.EstimatedPromptTokens
		acc.EstimatedCompletionTokens += entry.tokens.EstimatedCompletionTokens
		if entry.approximate {
			acc.Approximate = true
		}
	}

	dimCostsByModelNs := make(map[string][]*InferenceCost)
	for idk, acc := range merged {
		mc := findMatchingModelCost(idk.ServingModel, idk.Namespace)
		if mc == nil {
			continue
		}

		allocInRate := mc.InputCostPerMillionTokens[CostBasisAllocation]
		allocOutRate := mc.OutputCostPerMillionTokens[CostBasisAllocation]
		usageInRate := mc.InputCostPerMillionTokens[CostBasisUsage]
		usageOutRate := mc.OutputCostPerMillionTokens[CostBasisUsage]

		allocInputCost := acc.PromptTokens * (allocInRate / 1_000_000.0)
		allocOutputCost := acc.GenerationTokens * (allocOutRate / 1_000_000.0)
		allocTotalCost := allocInputCost + allocOutputCost

		usageInputCost := acc.PromptTokens * (usageInRate / 1_000_000.0)
		usageOutputCost := acc.GenerationTokens * (usageOutRate / 1_000_000.0)
		usageTotalCost := usageInputCost + usageOutputCost

		ic := &InferenceCost{
			Properties: InferenceCostProperties{
				ModelName:      mc.Properties.ModelName,
				ModelVersion:   mc.Properties.ModelVersion,
				Namespace:      mc.Properties.Namespace,
				Cluster:        mc.Properties.Cluster,
				Pod:            mc.Properties.Pod,
				Controller:     mc.Properties.Controller,
				ControllerKind: mc.Properties.ControllerKind,
				Container:      mc.Properties.Container,
				WorkloadType:   mc.Properties.WorkloadType,
				UserID:         idk.UserID,
				TenantID:       idk.TenantID,
				WorkloadID:     idk.WorkloadID,
			},
			Window:                     mc.Window,
			AllocationTotalCost:        allocTotalCost,
			UsageTotalCost:             usageTotalCost,
			PromptTokens:               acc.PromptTokens,
			GenerationTokens:           acc.GenerationTokens,
			TotalTokens:                acc.PromptTokens + acc.GenerationTokens,
			EstimatedPromptTokens:     acc.EstimatedPromptTokens,
			EstimatedCompletionTokens: acc.EstimatedCompletionTokens,
			InputCost:                  map[CostBasis]float64{CostBasisAllocation: allocInputCost, CostBasisUsage: usageInputCost},
			OutputCost:                 map[CostBasis]float64{CostBasisAllocation: allocOutputCost, CostBasisUsage: usageOutputCost},
			CostPerMillionTokens:       make(map[CostBasis]float64),
			InputCostPerMillionTokens:  make(map[CostBasis]float64),
			OutputCostPerMillionTokens: make(map[CostBasis]float64),
			AllocationMethod:           mc.AllocationMethod,
			Approximate:                acc.Approximate,
			Timestamp:                  mc.Timestamp,
		}

		key := modelNamespaceKey(mc.Properties.ModelName, mc.Properties.Namespace)
		dimCostsByModelNs[key] = append(dimCostsByModelNs[key], ic)
	}

	var finalDimCosts []*InferenceCost

	covThreshold := c.config.AttributionCoverageThreshold
	if covThreshold <= 0 {
		covThreshold = 0.9
	}
	overTolerance := c.config.AttributionOverattributionTolerance
	if overTolerance <= 0 {
		overTolerance = 0.1
	}

	for _, mc := range modelCosts {
		key := modelNamespaceKey(mc.Properties.ModelName, mc.Properties.Namespace)
		rows := dimCostsByModelNs[key]

		sumAllocCost := 0.0
		sumUsageCost := 0.0
		sumPromptTokens := 0.0
		sumCompTokens := 0.0
		realTokens := 0.0
		allAttributedTokens := 0.0

		for _, row := range rows {
			sumAllocCost += row.AllocationTotalCost
			sumUsageCost += row.UsageTotalCost
			sumPromptTokens += row.PromptTokens
			sumCompTokens += row.GenerationTokens
			rowTokens := row.PromptTokens + row.GenerationTokens
			allAttributedTokens += rowTokens
			if row.Properties.TenantID != "__unidentified__" && row.Properties.TenantID != "__unmetered__" && row.Properties.WorkloadID != "__unidentified__" {
				realTokens += rowTokens
			}
		}

		var remainderAlloc, remainderUsage float64
		if sumAllocCost > mc.AllocationTotalCost && sumAllocCost > 0 {
			scaleAlloc := mc.AllocationTotalCost / sumAllocCost
			for _, row := range rows {
				row.InputCost[CostBasisAllocation] *= scaleAlloc
				row.OutputCost[CostBasisAllocation] *= scaleAlloc
				row.AllocationTotalCost = row.InputCost[CostBasisAllocation] + row.OutputCost[CostBasisAllocation]
				row.Approximate = true
			}
			remainderAlloc = 0.0
		} else {
			remainderAlloc = math.Max(0, mc.AllocationTotalCost-sumAllocCost)
		}

		if sumUsageCost > mc.UsageTotalCost && sumUsageCost > 0 {
			scaleUsage := mc.UsageTotalCost / sumUsageCost
			for _, row := range rows {
				row.InputCost[CostBasisUsage] *= scaleUsage
				row.OutputCost[CostBasisUsage] *= scaleUsage
				row.UsageTotalCost = row.InputCost[CostBasisUsage] + row.OutputCost[CostBasisUsage]
				row.Approximate = true
			}
			remainderUsage = 0.0
		} else {
			remainderUsage = math.Max(0, mc.UsageTotalCost-sumUsageCost)
		}

		remPromptTokens := math.Max(0, mc.PromptTokens-sumPromptTokens)
		remCompTokens := math.Max(0, mc.GenerationTokens-sumCompTokens)
		remTotalTokens := remPromptTokens + remCompTokens

		var remAllocIn, remAllocOut, remUsageIn, remUsageOut float64
		if remTotalTokens > 0 {
			remAllocIn = remainderAlloc * (remPromptTokens / remTotalTokens)
			remAllocOut = remainderAlloc - remAllocIn
			remUsageIn = remainderUsage * (remPromptTokens / remTotalTokens)
			remUsageOut = remainderUsage - remUsageIn
		} else {
			remAllocIn = remainderAlloc / 2.0
			remAllocOut = remainderAlloc - remAllocIn
			remUsageIn = remainderUsage / 2.0
			remUsageOut = remainderUsage - remUsageIn
		}

		remRow := &InferenceCost{
			Properties: InferenceCostProperties{
				ModelName:      mc.Properties.ModelName,
				ModelVersion:   mc.Properties.ModelVersion,
				Namespace:      mc.Properties.Namespace,
				Cluster:        mc.Properties.Cluster,
				Pod:            mc.Properties.Pod,
				Controller:     mc.Properties.Controller,
				ControllerKind: mc.Properties.ControllerKind,
				Container:      mc.Properties.Container,
				WorkloadType:   mc.Properties.WorkloadType,
				TenantID:       "__unmetered__",
				WorkloadID:     "",
				UserID:         "",
			},
			Window:                     mc.Window,
			AllocationTotalCost:        remainderAlloc,
			UsageTotalCost:             remainderUsage,
			PromptTokens:               remPromptTokens,
			GenerationTokens:           remCompTokens,
			TotalTokens:                remTotalTokens,
			InputCost:                  map[CostBasis]float64{CostBasisAllocation: remAllocIn, CostBasisUsage: remUsageIn},
			OutputCost:                 map[CostBasis]float64{CostBasisAllocation: remAllocOut, CostBasisUsage: remUsageOut},
			CostPerMillionTokens:       make(map[CostBasis]float64),
			InputCostPerMillionTokens:  make(map[CostBasis]float64),
			OutputCostPerMillionTokens: make(map[CostBasis]float64),
			AllocationMethod:           mc.AllocationMethod,
			Timestamp:                  mc.Timestamp,
		}

		allRows := append(rows, remRow)
		for _, row := range allRows {
			if row.TotalTokens > 0 {
				row.CostPerMillionTokens[CostBasisAllocation] = row.AllocationTotalCost / row.TotalTokens * 1_000_000.0
				row.CostPerMillionTokens[CostBasisUsage] = row.UsageTotalCost / row.TotalTokens * 1_000_000.0
			}
			if row.PromptTokens > 0 {
				row.InputCostPerMillionTokens[CostBasisAllocation] = row.InputCost[CostBasisAllocation] / row.PromptTokens * 1_000_000.0
				row.InputCostPerMillionTokens[CostBasisUsage] = row.InputCost[CostBasisUsage] / row.PromptTokens * 1_000_000.0
			}
			if row.GenerationTokens > 0 {
				row.OutputCostPerMillionTokens[CostBasisAllocation] = row.OutputCost[CostBasisAllocation] / row.GenerationTokens * 1_000_000.0
				row.OutputCostPerMillionTokens[CostBasisUsage] = row.OutputCost[CostBasisUsage] / row.GenerationTokens * 1_000_000.0
			}
			finalDimCosts = append(finalDimCosts, row)
		}

		if mc.PromptTokens > 0 {
			cov := sumPromptTokens / mc.PromptTokens
			if cov < covThreshold {
				log.Warnf("InferenceCost: low prompt token coverage %.2f for model %s ns %s (attributed=%.0f, engine=%.0f)", cov, mc.Properties.ModelName, mc.Properties.Namespace, sumPromptTokens, mc.PromptTokens)
			}
			if cov > 1.0+overTolerance {
				log.Warnf("InferenceCost: prompt token over-attribution %.2f for model %s ns %s (attributed=%.0f, engine=%.0f)", cov, mc.Properties.ModelName, mc.Properties.Namespace, sumPromptTokens, mc.PromptTokens)
			}
		}
		if mc.GenerationTokens > 0 {
			cov := sumCompTokens / mc.GenerationTokens
			if cov < covThreshold {
				log.Warnf("InferenceCost: low completion token coverage %.2f for model %s ns %s (attributed=%.0f, engine=%.0f)", cov, mc.Properties.ModelName, mc.Properties.Namespace, sumCompTokens, mc.GenerationTokens)
			}
			if cov > 1.0+overTolerance {
				log.Warnf("InferenceCost: completion token over-attribution %.2f for model %s ns %s (attributed=%.0f, engine=%.0f)", cov, mc.Properties.ModelName, mc.Properties.Namespace, sumCompTokens, mc.GenerationTokens)
			}
		}
		if allAttributedTokens > 0 {
			identCov := realTokens / allAttributedTokens
			if identCov < covThreshold {
				log.Warnf("InferenceCost: low identity coverage %.2f for model %s ns %s (real=%.0f, total=%.0f)", identCov, mc.Properties.ModelName, mc.Properties.Namespace, realTokens, allAttributedTokens)
			}
		}
	}

	return finalDimCosts
}
