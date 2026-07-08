package prom

import (
	"fmt"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util"
)

// QueryInferencePromptTokens implements MetricsQuerier.QueryInferencePromptTokens
func (pds *PrometheusMetricsQuerier) QueryInferencePromptTokens(start, end time.Time) *source.Future[source.InferenceTokensResult] {
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)

	// Create a channel for the async result
	resultsChan := make(source.QueryResultsChan, 1)

	// Execute query asynchronously
	go func() {
		values, err := queryCounterDelta(ctx, "vllm:prompt_tokens_total", start, end)
		if err != nil {
			resultsChan <- &source.QueryResults{Error: err}
			return
		}

		// Convert map to QueryResults format
		results := mapToQueryResults(values)
		resultsChan <- &source.QueryResults{Results: results}
	}()

	return source.NewFuture(decodeInferenceTokensResult, resultsChan)
}

// QueryInferenceGenerationTokens implements MetricsQuerier.QueryInferenceGenerationTokens
func (pds *PrometheusMetricsQuerier) QueryInferenceGenerationTokens(start, end time.Time) *source.Future[source.InferenceTokensResult] {
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)

	resultsChan := make(source.QueryResultsChan, 1)

	go func() {
		values, err := queryCounterDelta(ctx, "vllm:generation_tokens_total", start, end)
		if err != nil {
			resultsChan <- &source.QueryResults{Error: err}
			return
		}

		results := mapToQueryResults(values)
		resultsChan <- &source.QueryResults{Results: results}
	}()

	return source.NewFuture(decodeInferenceTokensResult, resultsChan)
}

// QueryInferenceInputProcessingTime implements MetricsQuerier.QueryInferenceInputProcessingTime
func (pds *PrometheusMetricsQuerier) QueryInferenceInputProcessingTime(start, end time.Time) *source.Future[source.InferenceProcessingTimeResult] {
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)

	resultsChan := make(source.QueryResultsChan, 1)

	go func() {
		values, err := queryCounterDelta(ctx, "vllm:request_prefill_time_seconds_sum", start, end)
		if err != nil {
			resultsChan <- &source.QueryResults{Error: err}
			return
		}

		results := mapToQueryResults(values)
		resultsChan <- &source.QueryResults{Results: results}
	}()

	return source.NewFuture(decodeInferenceProcessingTimeResult, resultsChan)
}

// QueryInferenceOutputProcessingTime implements MetricsQuerier.QueryInferenceOutputProcessingTime
func (pds *PrometheusMetricsQuerier) QueryInferenceOutputProcessingTime(start, end time.Time) *source.Future[source.InferenceProcessingTimeResult] {
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)

	resultsChan := make(source.QueryResultsChan, 1)

	go func() {
		values, err := queryCounterDelta(ctx, "vllm:request_time_per_output_token_seconds_sum", start, end)
		if err != nil {
			resultsChan <- &source.QueryResults{Error: err}
			return
		}

		results := mapToQueryResults(values)
		resultsChan <- &source.QueryResults{Results: results}
	}()

	return source.NewFuture(decodeInferenceProcessingTimeResult, resultsChan)
}

// QueryInferenceCachedTokens implements MetricsQuerier.QueryInferenceCachedTokens
func (pds *PrometheusMetricsQuerier) QueryInferenceCachedTokens(start, end time.Time) *source.Future[source.InferenceTokensResult] {
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)

	resultsChan := make(source.QueryResultsChan, 1)

	go func() {
		values, err := queryCounterDelta(ctx, "vllm:prefix_cache_hits_total", start, end)
		if err != nil {
			resultsChan <- &source.QueryResults{Error: err}
			return
		}

		results := mapToQueryResults(values)
		resultsChan <- &source.QueryResults{Results: results}
	}()

	return source.NewFuture(decodeInferenceTokensResult, resultsChan)
}

// QueryInferenceCacheConfig implements MetricsQuerier.QueryInferenceCacheConfig
func (pds *PrometheusMetricsQuerier) QueryInferenceCacheConfig(t time.Time) *source.Future[source.InferenceCacheConfigResult] {
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)

	resultsChan := make(source.QueryResultsChan, 1)

	go func() {
		configs, err := queryCacheConfigs(ctx, t)
		if err != nil {
			resultsChan <- &source.QueryResults{Error: err}
			return
		}

		results := cacheConfigMapToQueryResults(configs)
		resultsChan <- &source.QueryResults{Results: results}
	}()

	return source.NewFuture(decodeInferenceCacheConfigResult, resultsChan)
}

// Decoder functions

func decodeInferenceTokensResult(result *source.QueryResult) *source.InferenceTokensResult {
	key, _ := result.GetString("key")
	value := result.Values[0].Value

	return &source.InferenceTokensResult{
		Values: map[string]float64{key: value},
	}
}

func decodeInferenceProcessingTimeResult(result *source.QueryResult) *source.InferenceProcessingTimeResult {
	key, _ := result.GetString("key")
	value := result.Values[0].Value

	return &source.InferenceProcessingTimeResult{
		Values: map[string]float64{key: value},
	}
}

func decodeInferenceCacheConfigResult(result *source.QueryResult) *source.InferenceCacheConfigResult {
	key, _ := result.GetString("key")
	enabled := result.Values[0].Value > 0

	return &source.InferenceCacheConfigResult{
		Configs: map[string]*source.InferenceCacheConfig{
			key: {PrefixCachingEnabled: enabled},
		},
	}
}

// Helper functions

// mapToQueryResults converts a map[string]float64 to []*QueryResult
func mapToQueryResults(values map[string]float64) []*source.QueryResult {
	results := make([]*source.QueryResult, 0, len(values))
	for key, value := range values {
		result := source.NewQueryResult(
			map[string]any{"key": key},
			[]*util.Vector{{Value: value}},
			nil,
		)
		results = append(results, result)
	}
	return results
}

// cacheConfigMapToQueryResults converts a map[string]*InferenceCacheConfig to []*QueryResult
func cacheConfigMapToQueryResults(configs map[string]*source.InferenceCacheConfig) []*source.QueryResult {
	results := make([]*source.QueryResult, 0, len(configs))
	for key, config := range configs {
		value := 0.0
		if config.PrefixCachingEnabled {
			value = 1.0
		}
		result := source.NewQueryResult(
			map[string]any{"key": key},
			[]*util.Vector{{Value: value}},
			nil,
		)
		results = append(results, result)
	}
	return results
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
// Negative deltas (counter resets) are treated as resets and the delta is set to the end value (post-reset activity).
func queryCounterDelta(ctx *Context, metric string, start, end time.Time) (map[string]float64, error) {
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
	endVals, err := queryInstantMetric(ctx, endQuery, effectiveEnd)
	if err != nil {
		return nil, fmt.Errorf("end-of-window query for %s: %w", metric, err)
	}

	// Query counter value at the start of the window.
	// Use a narrow 2m lookback here: we want the value just before the window
	// opens, not a stale value from much earlier that would undercount the delta.
	startQuery := fmt.Sprintf(`sum by (model_name, namespace) (last_over_time(%s[2m] @ %d))`, metric, startUnix)
	startVals, err := queryInstantMetric(ctx, startQuery, effectiveEnd)
	if err != nil {
		return nil, fmt.Errorf("start-of-window query for %s: %w", metric, err)
	}

	// Delta = end - start. If negative (counter reset), use endVal as a
	// lower bound to capture post-reset activity rather than reporting 0.
	out := make(map[string]float64, len(endVals))
	for key, endVal := range endVals {
		delta := endVal - startVals[key]
		if delta < 0 {
			// Counter reset detected: use endVal to capture post-reset activity
			delta = endVal
		}
		out[key] = delta
	}
	return out, nil
}

// queryCacheConfigs queries vllm:cache_config_info joined with token metrics
// to get enable_prefix_caching per (model_name, namespace).
// When the join produces no results for a model that has token data, a warning
// is emitted to aid diagnosis of pod-label mismatches.
func queryCacheConfigs(ctx *Context, t time.Time) (map[string]*source.InferenceCacheConfig, error) {
	// Join cache_config_info (has enable_prefix_caching label) with
	// prompt_tokens_total (has model_name) using namespace+pod as the join key.
	query := `
		max by (model_name, namespace, enable_prefix_caching) (
			sum by (model_name, namespace, pod) (vllm:prompt_tokens_total)
			* on (namespace, pod) group_left(enable_prefix_caching)
			max by (namespace, pod, enable_prefix_caching) (vllm:cache_config_info)
		)
	`

	raw, _, err := ctx.query(query, t)
	if err != nil {
		return nil, err
	}

	results := NewQueryResults(query, raw, source.ClusterKeyWithDefaults(ctx.config.ClusterLabel))
	if results.Error != nil {
		return nil, results.Error
	}

	out := make(map[string]*source.InferenceCacheConfig)
	for _, result := range results.Results {
		modelName, err := result.GetString("model_name")
		if err != nil || modelName == "" {
			continue
		}
		namespace, err := result.GetString("namespace")
		if err != nil || namespace == "" {
			namespace = "unknown"
		}
		enablePrefixCaching, err := result.GetString("enable_prefix_caching")
		if err != nil {
			continue
		}
		prefixCachingEnabled := strings.EqualFold(enablePrefixCaching, "true")
		key := modelNamespaceKey(modelName, namespace)
		out[key] = &source.InferenceCacheConfig{PrefixCachingEnabled: prefixCachingEnabled}
	}

	// Check for models that have token data but no cache config — likely a join
	// failure due to pod-label mismatch between cache_config_info and prompt_tokens_total.
	// Only run the diagnostic query when the join produced nothing; skip it on the happy path.
	if len(out) == 0 {
		rawQuery := `max by (namespace) (vllm:cache_config_info)`
		rawResult, _, rawErr := ctx.query(rawQuery, t)
		if rawErr == nil {
			diagResults := NewQueryResults(rawQuery, rawResult, source.ClusterKeyWithDefaults(ctx.config.ClusterLabel))
			if diagResults.Error == nil && len(diagResults.Results) > 0 {
				log.Warnf("InferenceCost: vllm:cache_config_info exists in Prometheus but the join with " +
					"vllm:prompt_tokens_total produced no results — likely a pod-label mismatch between " +
					"the two metrics (check that both carry matching 'namespace' and 'pod' labels). " +
					"prefix_caching_off detection will be disabled; allocation method will be 'compute_time'.")
			}
		}
	}

	return out, nil
}

// queryInstantMetric runs a Prometheus instant query evaluated at t and returns a
// map[model_name:namespace]value.
func queryInstantMetric(ctx *Context, query string, t time.Time) (map[string]float64, error) {
	raw, _, err := ctx.query(query, t)
	if err != nil {
		return nil, err
	}

	results := NewQueryResults(query, raw, source.ClusterKeyWithDefaults(ctx.config.ClusterLabel))
	if results.Error != nil {
		return nil, results.Error
	}

	out := make(map[string]float64, len(results.Results))
	for _, result := range results.Results {
		modelName, err := result.GetString("model_name")
		if err != nil || modelName == "" {
			continue
		}
		namespace, err := result.GetString("namespace")
		if err != nil || namespace == "" {
			namespace = "unknown"
		}
		if len(result.Values) == 0 {
			continue
		}
		value := result.Values[0].Value
		out[modelNamespaceKey(modelName, namespace)] = value
	}
	return out, nil
}

func modelNamespaceKey(modelName, namespace string) string {
	return modelName + ":" + namespace
}

// Made with Bob
