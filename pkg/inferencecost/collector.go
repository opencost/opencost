package inferencecost

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/filter/allocation"
	"github.com/opencost/opencost/core/pkg/filter/ops"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/source"
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
}

// NewCollector creates a Collector that uses the provided MetricsQuerier for
// inference metrics.
func NewCollector(config *Config, querier AllocationQuerier, metricsQuerier source.MetricsQuerier) (*Collector, error) {
	return &Collector{
		allocationQuerier: querier,
		metricsQuerier:    metricsQuerier,
		config:            config,
	}, nil
}

// CollectMetrics queries all data sources and returns one InferenceCost per
// model/namespace combination. start and end define the time window to query;
// the caller is responsible for choosing appropriate boundaries (e.g. the
// runner uses now-interval..now; the API uses the request window).
func (c *Collector) CollectMetrics(ctx context.Context, start, end time.Time) ([]*InferenceCost, error) {
	// --- Infrastructure costs from OpenCost allocation layer ---
	allocationCosts, err := c.queryAllocationCosts(ctx, start, end)
	if err != nil {
		return nil, fmt.Errorf("failed to query allocation costs: %w", err)
	}
	log.Infof("InferenceCost: collected allocation costs for %d model/namespace combinations", len(allocationCosts))

	// --- Token metrics from data source ---
	// Query all metrics concurrently using Futures
	promptTokensFuture := c.metricsQuerier.QueryInferencePromptTokens(start, end)
	generationTokensFuture := c.metricsQuerier.QueryInferenceGenerationTokens(start, end)
	inputTimeFuture := c.metricsQuerier.QueryInferenceInputProcessingTime(start, end)
	outputTimeFuture := c.metricsQuerier.QueryInferenceOutputProcessingTime(start, end)
	cachedTokensFuture := c.metricsQuerier.QueryInferenceCachedTokens(start, end)
	cacheConfigFuture := c.metricsQuerier.QueryInferenceCacheConfig(end)

	// Await required metrics (prompt and generation tokens)
	promptTokensResults, err := promptTokensFuture.Await()
	if err != nil {
		return nil, fmt.Errorf("failed to query prompt tokens: %w", err)
	}
	promptTokens := mergeTokenResults(promptTokensResults)

	generationTokensResults, err := generationTokensFuture.Await()
	if err != nil {
		return nil, fmt.Errorf("failed to query generation tokens: %w", err)
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

	return c.combineMetrics(allocationCosts, promptTokens, generationTokens,
		inputProcessingTime, outputProcessingTime, cachedTokens, cacheConfigs, start, end), nil
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
			// For usage cost: use TotalCost() from the ShareNone query (no idle)
			existing.usageTotalCost += alloc.TotalCost()
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
