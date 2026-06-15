package inferencecost

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

// AllocationQuerier is the subset of the cost model needed to fetch per-model
// infrastructure costs. Abstracted as an interface for testability.
type AllocationQuerier interface {
	// ComputeAllocation returns an AllocationSet for the given time window.
	ComputeAllocation(start, end time.Time) (*opencost.AllocationSet, error)
}

// Collector gathers per-model infrastructure costs from the OpenCost allocation
// layer and token/timing/cache metrics from Prometheus.
type Collector struct {
	allocationQuerier AllocationQuerier
	promClient        v1.API
	config            *Config
}

// NewCollector creates a Collector. Returns an error if the Prometheus client
// cannot be initialised (e.g. empty PrometheusURL).
func NewCollector(config *Config, querier AllocationQuerier) (*Collector, error) {
	if config.PrometheusURL == "" {
		return nil, fmt.Errorf("PrometheusURL is required for inference cost collector")
	}

	client, err := api.NewClient(api.Config{Address: config.PrometheusURL})
	if err != nil {
		return nil, fmt.Errorf("failed to create Prometheus client: %w", err)
	}

	return &Collector{
		allocationQuerier: querier,
		promClient:        v1.NewAPI(client),
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

	// --- Token metrics from Prometheus ---
	// Use a counter-delta approach: query the cumulative counter at both ends of
	// the window and subtract. This avoids the extrapolation inflation that
	// increase(metric[Xm]) produces when series have fewer samples than the
	// window duration (e.g. recently-started pods, multi-replica sum-by queries).
	// last_over_time(...[scrapeInterval]) pins each query to the nearest sample
	// at that timestamp without extrapolating across the full window.
	promptTokens, err := c.queryCounterDelta(ctx, "vllm:prompt_tokens_total", start, end)
	if err != nil {
		return nil, fmt.Errorf("failed to query prompt tokens: %w", err)
	}

	generationTokens, err := c.queryCounterDelta(ctx, "vllm:generation_tokens_total", start, end)
	if err != nil {
		return nil, fmt.Errorf("failed to query generation tokens: %w", err)
	}

	// --- Timing metrics (optional — degraded gracefully) ---
	inputProcessingTime, err := c.queryCounterDelta(ctx, "vllm:request_prefill_time_seconds_sum", start, end)
	if err != nil {
		log.Warnf("InferenceCost: failed to query input processing time (will use multiplier fallback): %v", err)
		inputProcessingTime = make(map[string]float64)
	}

	outputProcessingTime, err := c.queryCounterDelta(ctx, "vllm:request_time_per_output_token_seconds_sum", start, end)
	if err != nil {
		log.Warnf("InferenceCost: failed to query output processing time (will use multiplier fallback): %v", err)
		outputProcessingTime = make(map[string]float64)
	}

	// --- KV cache hits (optional — degraded gracefully) ---
	cacheHitBlocks, err := c.queryCounterDelta(ctx, "vllm:prefix_cache_hits_total", start, end)
	if err != nil {
		log.Warnf("InferenceCost: failed to query KV cache hits (cache denominator correction disabled): %v", err)
		cacheHitBlocks = make(map[string]float64)
	}

	return c.combineMetrics(allocationCosts, promptTokens, generationTokens,
		inputProcessingTime, outputProcessingTime, cacheHitBlocks, end), nil
}

// queryCounterDelta returns the net increase of a monotonic counter metric
// over [start, end] per (model_name, namespace).
//
// It uses the @ modifier to pin two instant queries to start and end,
// then subtracts. This avoids the extrapolation inflation produced by
// increase(metric[Xm]) when a series has fewer samples than the window
// (e.g. a pod that restarted mid-window, or a sum across many replicas
// where Prometheus extrapolates each series independently before summing).
//
// last_over_time(metric[2m] @ t) fetches the most recent sample within 2
// minutes of t. 2 minutes covers the default 30s scrape interval with margin.
// Series with no sample near start get a start-value of 0 (treated as new),
// which is the correct behaviour for pods that started mid-window.
// Negative deltas (counter resets) are clamped to 0 per series before summing.
func (c *Collector) queryCounterDelta(ctx context.Context, metric string, start, end time.Time) (map[string]float64, error) {
	startUnix := start.Unix()
	// Clamp end to now: last_over_time with a future @ timestamp returns no results.
	effectiveEnd := end
	if now := time.Now(); end.After(now) {
		effectiveEnd = now
	}
	endUnix := effectiveEnd.Unix()

	// The lookback for last_over_time must span the full window duration.
	// A model that was active earlier in the window but idle at query time
	// will have its last sample somewhere within the window — a narrow 2m
	// lookback would miss it entirely. Using the window duration as the
	// lookback guarantees we find the last sample that existed anywhere in
	// the window, while the @ pin ensures we don't extrapolate past end.
	windowDuration := effectiveEnd.Sub(start)
	windowMinutes := int(windowDuration.Minutes())
	if windowMinutes < 2 {
		windowMinutes = 2
	}

	// Query counter value at the end of the window.
	endQuery := fmt.Sprintf(`sum by (model_name, namespace) (last_over_time(%s[%dm] @ %d))`, metric, windowMinutes, endUnix)
	endVals, err := c.queryMetric(ctx, endQuery, effectiveEnd)
	if err != nil {
		return nil, fmt.Errorf("end-of-window query for %s: %w", metric, err)
	}

	// Query counter value at the start of the window.
	// Use a narrow 2m lookback here: we want the value just before the window
	// opens, not a stale value from much earlier that would undercount the delta.
	startQuery := fmt.Sprintf(`sum by (model_name, namespace) (last_over_time(%s[2m] @ %d))`, metric, startUnix)
	startVals, err := c.queryMetric(ctx, startQuery, effectiveEnd)
	if err != nil {
		return nil, fmt.Errorf("start-of-window query for %s: %w", metric, err)
	}

	// Delta = end - start, clamped to 0 to handle counter resets gracefully.
	out := make(map[string]float64, len(endVals))
	for key, endVal := range endVals {
		delta := endVal - startVals[key]
		if delta < 0 {
			delta = 0
		}
		out[key] = delta
	}
	return out, nil
}

// allocationResult holds the two cost figures derived from one Allocation.
type allocationResult struct {
	allocationTotalCost float64
	usageTotalCost      float64
	namespace           string
	cluster             string
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
		results[key] = &allocationResult{
			allocationTotalCost: allocResult.allocationTotalCost,
			usageTotalCost:      0, // Will be filled from usageCosts
			namespace:           allocResult.namespace,
			cluster:             allocResult.cluster,
		}
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
		if (result.allocationTotalCost > 0) {
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

	opts := &opencost.AllocationAggregationOptions{
		ShareIdle:    opencost.ShareWeighted,
		ShareSplit:   opencost.ShareWeighted,
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

	opts := &opencost.AllocationAggregationOptions{
		ShareIdle:    opencost.ShareNone,
		ShareSplit:   opencost.ShareNone,
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
		if alloc.Properties != nil {
			namespace = alloc.Properties.Namespace
			cluster = alloc.Properties.Cluster
		}

		key := modelNamespaceKey(modelName, namespace)

		if isAllocationCost {
			// For allocation cost: use TotalCost() which includes idle and shared
			results[key] = &allocationResult{
				allocationTotalCost: alloc.TotalCost(),
				usageTotalCost:      0, // Not used in this query
				namespace:           namespace,
				cluster:             cluster,
			}
		} else {
			// For usage cost: use TotalCost() from the ShareNone query (no idle)
			results[key] = &allocationResult{
				allocationTotalCost: 0, // Not used in this query
				usageTotalCost:      alloc.TotalCost(),
				namespace:           namespace,
				cluster:             cluster,
			}
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
//   1. Fully-qualified vLLM model name vs short allocation label:
//      "google/gemma-4-31B:llm-d-pic" -> "gemma-4-31B:llm-d-pic"
//   2. Fully-qualified vLLM model name vs short allocation label with a
//      different vendor/org prefix:
//      "MiniMaxAI/MiniMax-M2.7:llm-d-pic" -> "MiniMax-M2.7:llm-d-pic"
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

// combineMetrics joins all data sources into InferenceCost structs.
func (c *Collector) combineMetrics(
	allocCosts map[string]*allocationResult,
	promptTokens, generationTokens,
	inputProcessingTime, outputProcessingTime,
	cacheHitBlocks map[string]float64,
	now time.Time,
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
	
	cacheHitBlocks, remapped = reconcileTokenKeys(cacheHitBlocks, allocCosts)
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
	for k := range cacheHitBlocks {
		keys[k] = struct{}{}
	}

	results := make([]*InferenceCost, 0, len(keys))
	for key := range keys {
		// Skip keys that were remapped to avoid duplicate series
		if _, wasRemapped := remappedKeys[key]; wasRemapped {
			continue
		}
		
		modelName, namespace := parseKey(key)

		ic := &InferenceCost{
			Properties: InferenceCostProperties{
				ModelName: modelName,
				Namespace: namespace,
			},
			PromptTokens:         promptTokens[key],
			GenerationTokens:     generationTokens[key],
			InputProcessingTime:  inputProcessingTime[key],
			OutputProcessingTime: outputProcessingTime[key],
			CacheHitBlocks:       cacheHitBlocks[key],
			BlockSize:            c.config.KVCacheBlockSize,
			Timestamp:            now,
		}

		if ar, ok := allocCosts[key]; ok {
			ic.AllocationTotalCost = ar.allocationTotalCost
			ic.UsageTotalCost = ar.usageTotalCost
			ic.Properties.Cluster = ar.cluster
			if namespace == "" {
				ic.Properties.Namespace = ar.namespace
			}
		}

		ic.TotalTokens = ic.PromptTokens + ic.GenerationTokens
		ic.CachedTokens = ic.CacheHitBlocks * ic.BlockSize

		if ic.BlockSize > 0 && ic.CacheHitBlocks > 0 {
			ic.EffectiveInputTokens = ic.PromptTokens - ic.CachedTokens
			if ic.EffectiveInputTokens < 0 {
				ic.EffectiveInputTokens = 0
			}
		} else {
			ic.EffectiveInputTokens = ic.PromptTokens
		}

		results = append(results, ic)
	}
	return results
}

// queryMetric runs a Prometheus instant query evaluated at t and returns a
// map[model_name:namespace]value.
func (c *Collector) queryMetric(ctx context.Context, query string, t time.Time) (map[string]float64, error) {
	result, _, err := c.promClient.Query(ctx, query, t)
	if err != nil {
		return nil, err
	}

	vec, ok := result.(model.Vector)
	if !ok {
		return make(map[string]float64), nil
	}

	out := make(map[string]float64, len(vec))
	for _, sample := range vec {
		modelName := string(sample.Metric["model_name"])
		if modelName == "" {
			continue
		}
		namespace := string(sample.Metric["namespace"])
		if namespace == "" {
			namespace = "unknown"
		}
		out[modelNamespaceKey(modelName, namespace)] = float64(sample.Value)
	}
	return out, nil
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
