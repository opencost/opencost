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

// Inference Saturation Queries
//
// These query the model-server scheduler gauges standardized by the Gateway
// API Inference Extension Model Server Protocol: KV-cache utilization, queue
// depth (requests waiting), and running requests. Unlike host GPU utilization
// (which reads high for any healthy deployment), these measure how much of a
// model server's serving capacity the workload actually consumes. Results are
// per pod so per-replica saturation is preserved.
//
// Identity is the pod UID, matching how the KubeModel joins every other
// entity and how the DCGM queries already group (see queryFmtDCGMContainerUsageAvg
// in metricsquerier.go, which groups by UUID and pod_uid). Two label sources
// make that possible here:
//
//   - pod_uid comes from the scrape configuration. Model-server metrics are
//     already scraped with a config that attaches namespace and pod; adding
//     pod_uid is one more relabel rule from the __meta_kubernetes_pod_uid
//     meta-label that Kubernetes service discovery always provides. This is
//     the same way DCGM series carry pod_uid. See docs/inference-cost-tracking.md.
//   - namespace_uid has no equivalent meta-label, so it is joined in from
//     OpenCost's own namespace_info series on the namespace name.

// inferenceGroupBy is the identity these queries reduce to. The cluster label
// rides along so a multi-cluster Prometheus keeps clusters separate, the same
// way the DCGM queries append it to their own by-clause.
func inferenceGroupBy(clusterLabel string) string {
	return fmt.Sprintf(`model_name, pod_uid, namespace_uid, %s`, clusterLabel)
}

// joinNamespaceUID grafts namespace_uid onto a model-server expression from
// OpenCost's own namespace_info series, which carries the namespace UID in its
// "uid" label. namespace_info is an info metric with a constant value of 1, so
// multiplying by it preserves the sample value while adding the label.
// label_replace renames "uid" so it cannot collide with another UID label.
//
// Two properties this deliberately has:
//
// The match includes the cluster label. namespace_info is emitted by every
// OpenCost in a federated Prometheus, so matching on the namespace name alone
// makes two clusters that both have a namespace called "prod" a many-to-one
// duplicate match, which fails the whole query rather than mismatching quietly.
//
// The join is non-fatal. A bare `*` is an inner join, so a deployment that does
// not scrape OpenCost's own /metrics, or that has namespace_info disabled,
// would lose every model-server series rather than lose one label. The second
// branch keeps those series with namespace_uid unset, which matches the
// collector source and ValidateInferenceEngine, neither of which requires it.
//
// The fallback is `unless`, not a bare `or`. The joined series carries an extra
// label, so its label set never matches the unjoined one and `or` would emit
// both copies, splitting one pod across two result rows. `unless on (...)`
// selects only the series the join would have dropped.
func joinNamespaceUID(expr, clusterLabel string) string {
	nsInfo := fmt.Sprintf(
		`max by (namespace, namespace_uid, %s) (label_replace(namespace_info, "namespace_uid", "$1", "uid", "(.+)"))`,
		clusterLabel)
	return fmt.Sprintf(`((%[1]s) * on (namespace, %[2]s) group_left(namespace_uid) %[3]s or (%[1]s) unless on (namespace, %[2]s) %[3]s)`,
		expr, clusterLabel, nsInfo)
}

// inferenceSelector applies the configured cluster filter to a metric selector,
// matching how every DCGM query scopes its own metric.
func inferenceSelector(metric, clusterFilter string) string {
	if clusterFilter == "" {
		return metric
	}
	return fmt.Sprintf(`%s{%s}`, metric, clusterFilter)
}

// QueryInferenceKVCacheUsageAvg implements MetricsQuerier.QueryInferenceKVCacheUsageAvg
func (pds *PrometheusMetricsQuerier) QueryInferenceKVCacheUsageAvg(start, end time.Time) *source.Future[source.InferenceEngineMetricResult] {
	return pds.queryInferenceGauge("vllm:kv_cache_usage_perc", "avg", start, end)
}

// QueryInferenceKVCacheUsageMax implements MetricsQuerier.QueryInferenceKVCacheUsageMax
func (pds *PrometheusMetricsQuerier) QueryInferenceKVCacheUsageMax(start, end time.Time) *source.Future[source.InferenceEngineMetricResult] {
	return pds.queryInferenceGauge("vllm:kv_cache_usage_perc", "max", start, end)
}

// QueryInferenceQueueDepthAvg implements MetricsQuerier.QueryInferenceQueueDepthAvg
func (pds *PrometheusMetricsQuerier) QueryInferenceQueueDepthAvg(start, end time.Time) *source.Future[source.InferenceEngineMetricResult] {
	return pds.queryInferenceGauge("vllm:num_requests_waiting", "avg", start, end)
}

// QueryInferenceQueueDepthMax implements MetricsQuerier.QueryInferenceQueueDepthMax
func (pds *PrometheusMetricsQuerier) QueryInferenceQueueDepthMax(start, end time.Time) *source.Future[source.InferenceEngineMetricResult] {
	return pds.queryInferenceGauge("vllm:num_requests_waiting", "max", start, end)
}

// QueryInferenceRunningRequestsAvg implements MetricsQuerier.QueryInferenceRunningRequestsAvg
func (pds *PrometheusMetricsQuerier) QueryInferenceRunningRequestsAvg(start, end time.Time) *source.Future[source.InferenceEngineMetricResult] {
	return pds.queryInferenceGauge("vllm:num_requests_running", "avg", start, end)
}

// QueryInferenceRunningRequestsMax implements MetricsQuerier.QueryInferenceRunningRequestsMax.
// The running batch is bounded by the engine's concurrent-sequence limit
// (vLLM's max_num_seqs) as well as by the KV budget, and vLLM publishes no
// metric for that limit; the window max is what recovers it, since the gauge
// pins there whenever requests are waiting.
func (pds *PrometheusMetricsQuerier) QueryInferenceRunningRequestsMax(start, end time.Time) *source.Future[source.InferenceEngineMetricResult] {
	return pds.queryInferenceGauge("vllm:num_requests_running", "max", start, end)
}

// QueryInferenceKVCacheUsageP95 implements MetricsQuerier.QueryInferenceKVCacheUsageP95
func (pds *PrometheusMetricsQuerier) QueryInferenceKVCacheUsageP95(start, end time.Time) *source.Future[source.InferenceEngineMetricResult] {
	return pds.queryInferenceGaugeQuantile("vllm:kv_cache_usage_perc", 0.95, start, end)
}

// QueryInferenceQueueDepthP95 implements MetricsQuerier.QueryInferenceQueueDepthP95
func (pds *PrometheusMetricsQuerier) QueryInferenceQueueDepthP95(start, end time.Time) *source.Future[source.InferenceEngineMetricResult] {
	return pds.queryInferenceGaugeQuantile("vllm:num_requests_waiting", 0.95, start, end)
}

// QueryInferenceRunningRequestsP95 implements MetricsQuerier.QueryInferenceRunningRequestsP95
func (pds *PrometheusMetricsQuerier) QueryInferenceRunningRequestsP95(start, end time.Time) *source.Future[source.InferenceEngineMetricResult] {
	return pds.queryInferenceGaugeQuantile("vllm:num_requests_running", 0.95, start, end)
}

// queryInferenceGaugeQuantile runs quantile_over_time for a model-server
// scheduler gauge, grouped by (model_name, pod_uid, namespace_uid), pinned to
// the window end like the other inference queries.
func (pds *PrometheusMetricsQuerier) queryInferenceGaugeQuantile(metric string, phi float64, start, end time.Time) *source.Future[source.InferenceEngineMetricResult] {
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)

	resultsChan := make(source.QueryResultsChan, 1)

	go func() {
		// Clamp end to now: range selectors pinned to a future @ timestamp return no results.
		effectiveEnd := end
		if now := time.Now(); end.After(now) {
			effectiveEnd = now
		}

		windowDuration := effectiveEnd.Sub(start)
		windowMinutes := int(windowDuration.Minutes())
		if windowMinutes < 2 {
			windowMinutes = 2
		}

		inner := fmt.Sprintf(`quantile_over_time(%g, %s[%dm] @ %d)`,
			phi, inferenceSelector(metric, ctx.config.ClusterFilter), windowMinutes, effectiveEnd.Unix())
		query := fmt.Sprintf(`max by (%s) (%s)`,
			inferenceGroupBy(ctx.config.ClusterLabel), joinNamespaceUID(inner, ctx.config.ClusterLabel))

		raw, _, err := ctx.query(query, effectiveEnd)
		if err != nil {
			resultsChan <- &source.QueryResults{Error: fmt.Errorf("quantile-over-time query for %s: %w", metric, err)}
			return
		}

		results := NewQueryResults(query, raw, source.ClusterKeyWithDefaults(ctx.config.ClusterLabel))
		if results.Error != nil {
			resultsChan <- &source.QueryResults{Error: results.Error}
			return
		}

		resultsChan <- &source.QueryResults{Results: results.Results}
	}()

	return source.NewFuture(source.DecodeInferenceEngineMetricResult, resultsChan)
}

// QueryInferencePreemptions implements MetricsQuerier.QueryInferencePreemptions
func (pds *PrometheusMetricsQuerier) QueryInferencePreemptions(start, end time.Time) *source.Future[source.InferenceEngineMetricResult] {
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)

	resultsChan := make(source.QueryResultsChan, 1)

	go func() {
		results, err := queryCounterDeltaByReplica(ctx, "vllm:num_preemptions_total", start, end)
		if err != nil {
			resultsChan <- &source.QueryResults{Error: err}
			return
		}

		resultsChan <- &source.QueryResults{Results: results}
	}()

	return source.NewFuture(source.DecodeInferenceEngineMetricResult, resultsChan)
}

// replicaMetricValue carries one instant sample keyed by replica identity.
type replicaMetricValue struct {
	modelName    string
	podUID       string
	namespaceUID string
	value        float64
}

// queryCounterDeltaByReplica returns the net increase of a monotonic counter
// metric over [start, end] per (model_name, pod_uid, namespace_uid). Same
// @-pinned two-instant-query approach and counter-reset handling as
// queryCounterDelta, but grouped per replica and returned as labeled
// QueryResults for the shared InferenceEngineMetricResult decoder.
func queryCounterDeltaByReplica(ctx *Context, metric string, start, end time.Time) ([]*source.QueryResult, error) {
	startUnix := start.Unix()
	// Clamp end to now: last_over_time with a future @ timestamp returns no results.
	effectiveEnd := end
	if now := time.Now(); end.After(now) {
		effectiveEnd = now
	}
	endUnix := effectiveEnd.Unix()

	windowDuration := effectiveEnd.Sub(start)
	windowMinutes := int(windowDuration.Minutes())
	if windowMinutes < 2 {
		windowMinutes = 2
	}

	selector := inferenceSelector(metric, ctx.config.ClusterFilter)
	groupBy := inferenceGroupBy(ctx.config.ClusterLabel)

	endInner := fmt.Sprintf(`last_over_time(%s[%dm] @ %d)`, selector, windowMinutes, endUnix)
	endQuery := fmt.Sprintf(`sum by (%s) (%s)`, groupBy, joinNamespaceUID(endInner, ctx.config.ClusterLabel))
	endVals, err := queryInstantMetricByReplica(ctx, endQuery, effectiveEnd)
	if err != nil {
		return nil, fmt.Errorf("end-of-window query for %s: %w", metric, err)
	}

	// The start lookback spans the whole window, matching the end query rather
	// than using a narrow fixed one. A narrow lookback leaves a replica that
	// gapped for longer than it with no baseline at all, and a missing
	// baseline reads as zero, so the counter's entire lifetime value is
	// reported as this window's delta. Two minutes is a single missed scrape
	// at a 60s interval, and downsampled blocks may carry no raw sample that
	// close to an arbitrary timestamp.
	//
	// Widening it is safe here specifically because the pairing is per
	// (model_name, pod_uid) and the delta loop iterates the end values only: a
	// longer lookback can supply an older baseline for a pod present at the
	// end, but a pod that terminated before the window is never summed in. The
	// same widening over a sum-by-model rollup would not be safe.
	//
	// Residual: a replica that gapped for longer than the whole window still
	// has no baseline and still reports its lifetime value.
	startInner := fmt.Sprintf(`last_over_time(%s[%dm] @ %d)`, selector, windowMinutes, startUnix)
	startQuery := fmt.Sprintf(`sum by (%s) (%s)`, groupBy, joinNamespaceUID(startInner, ctx.config.ClusterLabel))
	startVals, err := queryInstantMetricByReplica(ctx, startQuery, effectiveEnd)
	if err != nil {
		return nil, fmt.Errorf("start-of-window query for %s: %w", metric, err)
	}

	results := make([]*source.QueryResult, 0, len(endVals))
	for key, endVal := range endVals {
		delta := endVal.value
		if startVal, ok := startVals[key]; ok {
			delta = endVal.value - startVal.value
			if delta < 0 {
				// Counter reset detected: use the end value to capture
				// post-reset activity rather than reporting 0.
				delta = endVal.value
			}
		}
		results = append(results, source.NewQueryResult(
			map[string]any{
				source.InferenceModelNameLabel: endVal.modelName,
				source.PodUIDLabel:             endVal.podUID,
				source.NamespaceUIDLabel:       endVal.namespaceUID,
			},
			[]*util.Vector{{Value: delta}},
			nil,
		))
	}
	return results, nil
}

// queryInstantMetricByReplica runs a Prometheus instant query evaluated at t
// and returns samples keyed by "model_name|pod_uid".
func queryInstantMetricByReplica(ctx *Context, query string, t time.Time) (map[string]replicaMetricValue, error) {
	raw, _, err := ctx.query(query, t)
	if err != nil {
		return nil, err
	}

	results := NewQueryResults(query, raw, source.ClusterKeyWithDefaults(ctx.config.ClusterLabel))
	if results.Error != nil {
		return nil, results.Error
	}

	out := make(map[string]replicaMetricValue, len(results.Results))
	for _, result := range results.Results {
		modelName, err := result.GetString(source.InferenceModelNameLabel)
		if err != nil || modelName == "" {
			continue
		}
		podUID, _ := result.GetString(source.PodUIDLabel)
		if podUID == "" {
			continue
		}
		namespaceUID, _ := result.GetString(source.NamespaceUIDLabel)
		if len(result.Values) == 0 {
			continue
		}
		key := modelName + "|" + podUID
		out[key] = replicaMetricValue{
			modelName:    modelName,
			podUID:       podUID,
			namespaceUID: namespaceUID,
			value:        result.Values[0].Value,
		}
	}
	return out, nil
}

// queryInferenceGauge runs a window aggregation (avg or max) of a model-server
// scheduler gauge, grouped by (model_name, pod_uid, namespace_uid).
func (pds *PrometheusMetricsQuerier) queryInferenceGauge(metric, agg string, start, end time.Time) *source.Future[source.InferenceEngineMetricResult] {
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)

	resultsChan := make(source.QueryResultsChan, 1)

	go func() {
		results, err := queryGaugeOverTime(ctx, metric, agg, start, end)
		if err != nil {
			resultsChan <- &source.QueryResults{Error: err}
			return
		}

		resultsChan <- &source.QueryResults{Results: results}
	}()

	return source.NewFuture(source.DecodeInferenceEngineMetricResult, resultsChan)
}

// queryGaugeOverTime evaluates `agg by (model_name, pod_uid, namespace_uid)
// (agg_over_time(metric[window] @ end))` as an instant query pinned to the end
// of the window, mirroring the clamping behaviour of queryCounterDelta.
func queryGaugeOverTime(ctx *Context, metric, agg string, start, end time.Time) ([]*source.QueryResult, error) {
	// Clamp end to now: range selectors pinned to a future @ timestamp return no results.
	effectiveEnd := end
	if now := time.Now(); end.After(now) {
		effectiveEnd = now
	}

	windowDuration := effectiveEnd.Sub(start)
	windowMinutes := int(windowDuration.Minutes())
	if windowMinutes < 2 {
		windowMinutes = 2
	}

	inner := fmt.Sprintf(`%s_over_time(%s[%dm] @ %d)`,
		agg, inferenceSelector(metric, ctx.config.ClusterFilter), windowMinutes, effectiveEnd.Unix())
	query := fmt.Sprintf(`%s by (%s) (%s)`,
		agg, inferenceGroupBy(ctx.config.ClusterLabel), joinNamespaceUID(inner, ctx.config.ClusterLabel))

	raw, _, err := ctx.query(query, effectiveEnd)
	if err != nil {
		return nil, fmt.Errorf("gauge-over-time query for %s: %w", metric, err)
	}

	results := NewQueryResults(query, raw, source.ClusterKeyWithDefaults(ctx.config.ClusterLabel))
	if results.Error != nil {
		return nil, results.Error
	}

	return results.Results, nil
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
