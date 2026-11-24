package prom

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util"
	"github.com/opencost/opencost/modules/prometheus-source/pkg/env"
	"gopkg.in/yaml.v2"
)

// ProducerScope defines scope filtering for an attribution producer.
type ProducerScope struct {
	ServingModels        []string `yaml:"servingModels" json:"servingModels"`
	ExcludeServingModels []string `yaml:"excludeServingModels" json:"excludeServingModels"`
	Namespaces           []string `yaml:"namespaces" json:"namespaces"`
}

// ProducerMapping defines the configuration for a single attribution producer.
type ProducerMapping struct {
	Name                        string        `yaml:"name" json:"name"`
	InputTokensSeries           string        `yaml:"inputTokensSeries" json:"inputTokensSeries"`
	OutputTokensSeries          string        `yaml:"outputTokensSeries" json:"outputTokensSeries"`
	InputTokensEstimatedSeries  string        `yaml:"inputTokensEstimatedSeries" json:"inputTokensEstimatedSeries"`
	OutputTokensEstimatedSeries string        `yaml:"outputTokensEstimatedSeries" json:"outputTokensEstimatedSeries"`
	TenantLabel                 string        `yaml:"tenantLabel" json:"tenantLabel"`
	WorkloadLabel               string        `yaml:"workloadLabel" json:"workloadLabel"`
	UserLabel                   string        `yaml:"userLabel" json:"userLabel"`
	ModelLabel                  string        `yaml:"modelLabel" json:"modelLabel"`
	NamespaceLabel              string        `yaml:"namespaceLabel" json:"namespaceLabel"`
	IdentitySentinel            string        `yaml:"identitySentinel" json:"identitySentinel"`
	UserSentinel                string        `yaml:"userSentinel" json:"userSentinel"`
	Scope                       ProducerScope `yaml:"scope" json:"scope"`
}

// AttributionConfigFile represents the YAML configuration file for multiple producers.
type AttributionConfigFile struct {
	Producers []ProducerMapping `yaml:"producers" json:"producers"`
}

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
		// DECODE-phase seconds. NOT request_time_per_output_token_seconds_sum, which
		// sums per-request mean seconds-per-token rather than a duration.
		values, err := queryCounterDelta(ctx, "vllm:request_decode_time_seconds_sum", start, end)
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

// QueryInferenceDimensionTokens implements MetricsQuerier.QueryInferenceDimensionTokens
func (pds *PrometheusMetricsQuerier) QueryInferenceDimensionTokens(start, end time.Time) *source.Future[source.InferenceDimensionResult] {
	if !env.IsInferenceAttributionEnabled() {
		return source.NewFutureFrom([]*source.InferenceDimensionResult{{
			Values: make(map[source.InferenceDimensionKey]*source.InferenceDimensionTokens),
		}})
	}

	mappings, err := loadProducerMappings()
	if err != nil || len(mappings) == 0 {
		if err != nil {
			log.Errorf("InferenceCost: failed to load attribution configuration: %v", err)
		}
		return source.NewFutureFrom([]*source.InferenceDimensionResult{{
			Values: make(map[source.InferenceDimensionKey]*source.InferenceDimensionTokens),
		}})
	}

	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	resultsChan := make(source.QueryResultsChan, 1)

	go func() {
		values := make(map[source.InferenceDimensionKey]*source.InferenceDimensionTokens)

		for _, mapping := range mappings {
			if mapping.InputTokensSeries != "" {
				if err := queryDimensionSeries(ctx, mapping, mapping.InputTokensSeries, start, end, true, false, values); err != nil {
					log.Warnf("InferenceCost: query input tokens series %s failed for producer %s: %v", mapping.InputTokensSeries, mapping.Name, err)
				}
			}
			if mapping.OutputTokensSeries != "" {
				if err := queryDimensionSeries(ctx, mapping, mapping.OutputTokensSeries, start, end, false, false, values); err != nil {
					log.Warnf("InferenceCost: query output tokens series %s failed for producer %s: %v", mapping.OutputTokensSeries, mapping.Name, err)
				}
			}
			if mapping.InputTokensEstimatedSeries != "" {
				if err := queryDimensionSeries(ctx, mapping, mapping.InputTokensEstimatedSeries, start, end, true, true, values); err != nil {
					log.Warnf("InferenceCost: query estimated input tokens series %s failed for producer %s: %v", mapping.InputTokensEstimatedSeries, mapping.Name, err)
				}
			}
			if mapping.OutputTokensEstimatedSeries != "" {
				if err := queryDimensionSeries(ctx, mapping, mapping.OutputTokensEstimatedSeries, start, end, false, true, values); err != nil {
					log.Warnf("InferenceCost: query estimated output tokens series %s failed for producer %s: %v", mapping.OutputTokensEstimatedSeries, mapping.Name, err)
				}
			}
		}

		results := dimensionMapToQueryResults(values)
		resultsChan <- &source.QueryResults{Results: results}
	}()

	return source.NewFuture(source.DecodeInferenceDimensionResult, resultsChan)
}

func loadProducerMappings() ([]*ProducerMapping, error) {
	configPath := env.GetInferenceAttributionConfigPath()
	if configPath != "" {
		data, err := os.ReadFile(configPath)
		if err != nil {
			return nil, fmt.Errorf("reading attribution config file at %s: %w", configPath, err)
		}
		var cfg AttributionConfigFile
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			return nil, fmt.Errorf("parsing attribution config YAML: %w", err)
		}
		var mappings []*ProducerMapping
		for i := range cfg.Producers {
			p := cfg.Producers[i]
			applyProducerDefaults(&p)
			mappings = append(mappings, &p)
		}
		return mappings, nil
	}

	inputSeries := env.GetInferenceAttributionInputTokensSeries()
	outputSeries := env.GetInferenceAttributionOutputTokensSeries()
	if inputSeries == "" && outputSeries == "" {
		return nil, nil
	}

	mapping := &ProducerMapping{
		Name:                        "default",
		InputTokensSeries:           inputSeries,
		OutputTokensSeries:          outputSeries,
		InputTokensEstimatedSeries:  env.GetInferenceAttributionInputTokensEstimatedSeries(),
		OutputTokensEstimatedSeries: env.GetInferenceAttributionOutputTokensEstimatedSeries(),
		TenantLabel:                 env.GetInferenceAttributionTenantLabel(),
		WorkloadLabel:               env.GetInferenceAttributionWorkloadLabel(),
		UserLabel:                   env.GetInferenceAttributionUserLabel(),
		ModelLabel:                  env.GetInferenceAttributionModelLabel(),
		NamespaceLabel:              env.GetInferenceAttributionNamespaceLabel(),
		IdentitySentinel:            env.GetInferenceAttributionIdentitySentinel(),
		UserSentinel:                env.GetInferenceAttributionUserSentinel(),
	}
	applyProducerDefaults(mapping)
	return []*ProducerMapping{mapping}, nil
}

func applyProducerDefaults(p *ProducerMapping) {
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
}

// seriesIdentity returns a stable identifier for a Prometheus series built from
// all of its labels except the metric name. The metric name may or may not be
// kept by the query depending on the Prometheus version, so it is excluded.
// Each label name and value is quoted so that separators inside values cannot
// make two different label sets produce the same identity.
func seriesIdentity(metric map[string]interface{}) string {
	parts := make([]string, 0, len(metric))
	for k, v := range metric {
		if k == "__name__" {
			continue
		}
		parts = append(parts, fmt.Sprintf("%q=%q", k, fmt.Sprintf("%v", v)))
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

// counterIncrease returns the increase of a counter across the ordered samples,
// starting from the baseline value. Every drop between consecutive samples is
// a counter reset, after which the new sample value is the post-reset
// activity, so resets in the middle of the window are not lost.
func counterIncrease(baseline float64, samples []*util.Vector) float64 {
	prev := baseline
	total := 0.0
	for _, s := range samples {
		if s.Value >= prev {
			total += s.Value - prev
		} else {
			total += s.Value
		}
		prev = s.Value
	}
	return total
}

// queryDimensionSeriesDeltas returns, for every series of a counter, the exact
// increase between start and end together with the series' labels.
//
// It deliberately does not use increase(metric[Ns]). increase() extrapolates
// the samples inside the window to the whole window, so a window holding only
// a couple of scrapes overstates a single step in the counter by up to
// window/scrape-interval (2x for a 60s window and a 30s scrape). Instead the
// raw samples in the window are fetched and the increase is summed between
// consecutive samples, starting from last_over_time at the window start, so the
// result is exact and counter resets anywhere in the window are accounted for.
//
// The delta is taken per series, before any grouping, so a counter reset on one
// replica does not corrupt the others. A series with no sample at the start of
// the window is treated as new (start value 0).
func queryDimensionSeriesDeltas(ctx *Context, metric string, start, end time.Time) ([]*source.QueryResult, []float64, error) {
	effectiveEnd := end
	if now := time.Now(); end.After(now) {
		effectiveEnd = now
	}
	endUnix := effectiveEnd.Unix()
	startUnix := start.Unix()

	// Raw samples inside the window, so a series that was active earlier in the
	// window but is idle at its end is still found and resets are visible.
	windowSeconds := int64(effectiveEnd.Sub(start).Seconds())
	if windowSeconds < 1 {
		windowSeconds = 1
	}

	endQuery := fmt.Sprintf("%s[%ds] @ %d", metric, windowSeconds, endUnix)
	rawEnd, _, err := ctx.query(endQuery, effectiveEnd)
	if err != nil {
		return nil, nil, fmt.Errorf("window query for %s: %w", metric, err)
	}
	endResults := NewQueryResults(endQuery, rawEnd, source.ClusterKeyWithDefaults(ctx.config.ClusterLabel))
	if endResults.Error != nil {
		return nil, nil, endResults.Error
	}

	// Narrow lookback at the start: we want the value just before the window
	// opens, not a stale value from much earlier.
	startQuery := fmt.Sprintf("last_over_time(%s[2m] @ %d)", metric, startUnix)
	rawStart, _, err := ctx.query(startQuery, effectiveEnd)
	if err != nil {
		return nil, nil, fmt.Errorf("start-of-window query for %s: %w", metric, err)
	}
	startResults := NewQueryResults(startQuery, rawStart, source.ClusterKeyWithDefaults(ctx.config.ClusterLabel))
	if startResults.Error != nil {
		return nil, nil, startResults.Error
	}

	startValues := make(map[string]float64, len(startResults.Results))
	for _, r := range startResults.Results {
		if len(r.Values) == 0 {
			continue
		}
		startValues[seriesIdentity(r.Metric)] = r.Values[0].Value
	}

	series := make([]*source.QueryResult, 0, len(endResults.Results))
	deltas := make([]float64, 0, len(endResults.Results))
	for _, r := range endResults.Results {
		if len(r.Values) == 0 {
			continue
		}
		series = append(series, r)
		deltas = append(deltas, counterIncrease(startValues[seriesIdentity(r.Metric)], r.Values))
	}
	return series, deltas, nil
}

func queryDimensionSeries(
	ctx *Context,
	mapping *ProducerMapping,
	metric string,
	start, end time.Time,
	isInput bool,
	isEstimated bool,
	values map[source.InferenceDimensionKey]*source.InferenceDimensionTokens,
) error {
	if metric == "" {
		return nil
	}
	series, deltas, err := queryDimensionSeriesDeltas(ctx, metric, start, end)
	if err != nil {
		return fmt.Errorf("dimension query for %s: %w", metric, err)
	}

	for i, result := range series {
		tenantID, _ := result.GetString(mapping.TenantLabel)
		var workloadID string
		if mapping.WorkloadLabel != "" {
			workloadID, _ = result.GetString(mapping.WorkloadLabel)
		}
		var userID string
		if mapping.UserLabel != "" {
			userID, _ = result.GetString(mapping.UserLabel)
		}
		servingModel, _ := result.GetString(mapping.ModelLabel)
		namespace, _ := result.GetString(mapping.NamespaceLabel)

		delta := deltas[i]

		key := source.InferenceDimensionKey{
			Producer:     mapping.Name,
			UserID:       userID,
			TenantID:     tenantID,
			WorkloadID:   workloadID,
			ServingModel: servingModel,
			Namespace:    namespace,
		}

		entry, ok := values[key]
		if !ok {
			entry = &source.InferenceDimensionTokens{}
			values[key] = entry
		}

		// Estimated tokens are a separate series from exact tokens, so they are
		// added to the totals as well as to the estimated subtotals (which are
		// subsets of the totals).
		if isInput {
			entry.PromptTokens += delta
			if isEstimated {
				entry.EstimatedPromptTokens += delta
			}
		} else {
			entry.CompletionTokens += delta
			if isEstimated {
				entry.EstimatedCompletionTokens += delta
			}
		}
	}
	return nil
}

func dimensionMapToQueryResults(values map[source.InferenceDimensionKey]*source.InferenceDimensionTokens) []*source.QueryResult {
	results := make([]*source.QueryResult, 0, len(values))
	for key, tokens := range values {
		metric := map[string]any{
			"producer":                    key.Producer,
			"user_id":                     key.UserID,
			"tenant_id":                   key.TenantID,
			"workload_id":                 key.WorkloadID,
			"serving_model":               key.ServingModel,
			"namespace":                   key.Namespace,
			"prompt_tokens":               tokens.PromptTokens,
			"completion_tokens":           tokens.CompletionTokens,
			"estimated_prompt_tokens":     tokens.EstimatedPromptTokens,
			"estimated_completion_tokens": tokens.EstimatedCompletionTokens,
		}
		result := source.NewQueryResult(
			metric,
			[]*util.Vector{{Value: tokens.PromptTokens + tokens.CompletionTokens}},
			nil,
		)
		results = append(results, result)
	}
	return results
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
