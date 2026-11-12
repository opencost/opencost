# OpenCost Inference Costs: Attribution Dimensions Implementation Plan

Author(s): @simanadler

**Status:** Draft

Add `tenant_id`, `workload_id`, and `user_id` tracking to the OpenCost inference cost capabilities, consuming attribution metrics produced by **any** serving stack that meets the [Inference Attribution Requirements](./inference-attribution-requirements.md).

This plan is producer-neutral. Metric names and label names are configuration, and no producer-specific discovery is needed.

Related documents:

- [Inference Attribution Requirements](./inference-attribution-requirements.md): what a serving stack must provide. This plan implements the OpenCost side of that contract (its Sections 7.3 and 8).
- [AI Inference Cost Tracking](./inference-cost-tracking.md): the existing model-level cost solution this plan extends.

---

## Business need

As self-hosted AI model deployments grow to serve multiple teams, applications and external customers, platform operators need answers that raw infrastructure metrics cannot provide:

- **Chargeback.** Which tenant or workload is responsible for what share of the GPU bill?
- **Showback.** Give each team visibility into its own consumption without exposing the full cluster cost view.
- **Capacity planning.** Which workloads are growing fastest, and which models are they driving load on?
- **Quota enforcement.** Detect when a single user or workload is crowding out others before it becomes an incident.

OpenCost already reports inference cost per model and namespace ([AI Inference Cost Tracking](./inference-cost-tracking.md)). It cannot say who caused that cost, because request identity (who called) is visible only in the request path, while GPU cost is visible only in the cluster. Attribution joins the two:

```
attributed cost = tokens(tenant, workload, user, model) x cost-per-token(model)
```

The serving stack owns identity and volume. OpenCost owns cost. The serving-stack side of this split is specified in the [Inference Attribution Requirements](./inference-attribution-requirements.md).

### Two kinds of cost

| Cost type | What it covers | Needs attribution metrics? |
|---|---|---|
| **Infrastructure cost** | Total GPU, CPU and RAM spend per model per namespace | No. OpenCost derives it from engine metrics and Kubernetes allocation. |
| **Attributed cost** | Infrastructure cost broken down by tenant, workload or user | Yes. Only a metering layer that sees both identity and response token counts can supply it. |

For attributed cost to be accurate, every inference request must pass through the metering layer. A client that bypasses it is counted in the engine's token metrics, and therefore in infrastructure cost, but in no attributed series. The coverage check (§1.5) detects this gap.

### New outputs

**Prometheus metric.** A new gauge `llm_dimension_hourly_cost`, exported only when attribution is enabled (§1.6, §1.9):

```
llm_dimension_hourly_cost{
  model_name="meta-llama/Llama-3-70b-instruct",
  namespace="llm-d-prod",
  cost_basis="allocation",
  tenant_id="acme-corp/engineering",
  workload_id="rag-agent-v2",
  user_id="alice"
} 0.0412
```

`cost_basis=allocation` reconciles to the bill. `cost_basis=usage` is active compute only. `user_id` is empty when the producer does not emit it.

Cost that no attributed series claims is not dropped. For each model and namespace, an extra row with `tenant_id="__unmetered__"` carries the remainder: the model's cost minus everything attributed to a tenant. Tenant rows plus this row add up to the model's cost under each basis. In practice the remainder is mostly the cost of requests that bypassed the metering layer, with defined edge cases (§1.5).

```promql
# Top 5 tenants by attributed cost
topk(5, sum by (tenant_id) (llm_dimension_hourly_cost{cost_basis="allocation"}))
```

**API.** `/inferenceCost/total` and `/inferenceCost/timeseries` gain `tenant_id`, `workload_id` and `user_id` as values for the `aggregate` and `filter` parameters. They combine with the existing dimensions:

```
# Cost by tenant across all models, last 7 days:
GET /inferenceCost/total?window=7d&aggregate=tenant_id&costBasis=allocation

# Hourly per-workload timeseries for one model:
GET /inferenceCost/timeseries?window=24h&accumulate=hour
  &filter=model_name:"meta-llama/Llama-3-70b"&aggregate=workload_id

# Total cost for a single user over 30 days:
GET /inferenceCost/total?window=30d&filter=user_id:"alice"
```

**Authorization for `user_id`.** Per-person spend is sensitive, and `/inferenceCost` has no tenancy model of its own. `aggregate` and `filter` on `user_id` are therefore default-deny: they require both `INFERENCE_ATTRIBUTION_USER_AGGREGATION_ENABLED=true` (§1.9, default false) and admin authentication (the existing `ADMIN_TOKEN` bearer mechanism). Non-admin callers, and any caller when the flag is off, receive a validation error naming the restriction. `tenant_id` and `workload_id` are not restricted. The same applies on the metrics path: the `user_id` label on `llm_dimension_hourly_cost` is readable by anyone with Prometheus query access, so a deployment exposing per-user cost must restrict the metric with Prometheus RBAC or disable the user label (§1.9). Hiding per-user dollars in a UI is moot if the API and the metric still return them.

The response shape is unchanged. `properties` gains three optional fields, `tenant_id`, `workload_id` and `user_id`, omitted when empty (§1.4).

### Relationship to OpenCost's existing allocation dimensions

OpenCost's allocation API already supports organisational attribution through aliased label properties (`department`, `team`, `owner`, `product`, `environment`) backed by Kubernetes pod labels. The new dimensions complement them and cannot be replaced by them:

| Characteristic | Aliased pod labels | New inference dimensions |
|---|---|---|
| Source | Pod and namespace labels, set at deploy time | Request identity, set per request by the caller or gateway |
| Granularity | One value per pod | Per request, aggregated per identity |
| Visible to the allocation layer | Yes | No. Request identity never reaches Kubernetes or OpenCost allocation. |
| Cost attribution | Direct grouping of allocation costs | Indirect: model-level per-million rates applied to per-dimension token counts |

A pod label cannot work here: a single model pod serves many tenants at once, and a pod label holds one value. Workload identity is set by the calling application at runtime, and user identity (often an email address) is not a valid Kubernetes label value. The two mechanisms can be combined, for example `?filter=namespace:"llm-d-prod"&aggregate=tenant_id`.

---

## 0. Relationship to the requirements document

| Requirements doc | This plan |
|---|---|
| F1, F2, F3: `tenant_id`, `workload_id`, `user_id` | Dimension labels on the attributed token series (§1.3) |
| F4, F5: prompt and completion tokens | The two token series OpenCost queries (§1.3) |
| F7: served model | `serving_model` label, the join key to model-level rates (§1.5) |
| F8: deployment namespace | Deployment namespace label, the second join key (§1.3, §2) |
| §7.3: aggregate metric contract | Configurable metric and label names (§1.3, §1.9) |
| §8: what OpenCost does | §1.5 to §1.7 |
| F6 requested model, F9 to F12 | Not consumed by OpenCost. They matter to the metering layer, not to cost |

Anything in the requirements document about identity derivation, failure semantics, headers or privacy is the serving stack's responsibility and is not implemented here. OpenCost consumes aggregate series only.

---

## 1. Proposed Implementation

### 1.1 New types (`core/pkg/source/inference_results.go`)

```go
// InferenceDimensionKey identifies a (user_id, tenant_id, workload_id, serving_model, namespace)
// combination. UserID is empty when the user label is not configured or absent from the series.
type InferenceDimensionKey struct {
    UserID       string
    TenantID     string
    WorkloadID   string
    ServingModel string
    Namespace    string // deployment namespace; see §1.3 for how it is labelled
}

// InferenceDimensionTokens holds the prompt and completion token counts for one dimension key.
type InferenceDimensionTokens struct {
    PromptTokens     float64
    CompletionTokens float64
}

// InferenceDimensionResult holds per-dimension token counts from the attribution series.
type InferenceDimensionResult struct {
    Values map[InferenceDimensionKey]*InferenceDimensionTokens
}
```

The types are independent of the producer.

### 1.2 `MetricsQuerier` interface extension (`core/pkg/source/datasource.go`)

One new query method and constant:

```go
QueryInferenceDimensionTokens = "QueryInferenceDimensionTokens"

// QueryInferenceDimensionTokens returns prompt and completion token deltas over [start, end]
// from the configured attribution series, broken down by the configured identity labels.
QueryInferenceDimensionTokens(start, end time.Time) *Future[InferenceDimensionResult]
```

There is no producer-discovery query. The user label is a single, global configuration (§1.9), and its absence from a series is handled by the query (§1.3).

Stubs required in: `core/pkg/source/noop.go`, `record.go`, `mock.go`, and `modules/collector-source/pkg/collector/metricsquerier.go`.
Decoder required in: `core/pkg/source/decoders.go` (`DecodeInferenceDimensionResult`).

### 1.3 Prometheus queries (`modules/prometheus-source/pkg/prom/inference_queries.go`)

#### 1.3.1 Configurable series

OpenCost does not know the producer's metric names. They come from configuration (§1.9):

- Input-token series and output-token series. Each may be a counter (`..._total`) or a histogram `_sum` series. Both behave as monotonically increasing counters, so the same delta pattern works for either. The configured value is the full series name.
- Optional estimated-token series (input and output), for producers that separate estimated counts per the requirements document §7.3 (sibling-series variant). Estimated tokens count as attributed and are exported as a separate share (§1.6).
- Label names for tenant, workload, user, serving model and deployment namespace. Defaults match the requirements document: `tenant_id`, `workload_id`, `user_id`, `serving_model`, `namespace`.

The generated PromQL substitutes configured names. In the examples below, `<tenant>`, `<workload>`, `<user>`, `<model>` and `<ns>` stand for the configured label names, and `<input_series>` for the configured input series.

#### 1.3.2 `QueryInferenceDimensionTokens`: counter delta strategy

Four queries in total (input and output, each at the end and start of the window), following the same `last_over_time` delta pattern as `queryCounterDelta`:

- **End of window** (input tokens):
  ```promql
  sum by (<user>, <tenant>, <workload>, <model>, <ns>) (
    last_over_time(<input_series>[<window>m] @ <end_unix>)
  )
  ```
- **Start of window** (narrow lookback to anchor the delta):
  ```promql
  sum by (<user>, <tenant>, <workload>, <model>, <ns>) (
    last_over_time(<input_series>[2m] @ <start_unix>)
  )
  ```

`<window>m` = `windowDuration.Minutes()` (minimum 2), matching the `queryCounterDelta` convention. Both queries are issued at `effectiveEnd` (clamped to `time.Now()`) to avoid future-timestamp errors. The same pattern applies to the output series. The helper `queryDimensionCounterDelta` encapsulates the start and end pair and is called twice. Results are merged into one `InferenceDimensionResult`.

**Deltas are computed per producer series, reset-aware, before grouping by identity.** The `by` clauses above show only the identity labels, but the query must first retain the producer's own series identity (at minimum `job` and `instance`), compute the delta per producer series, and only then sum by the identity labels. Two rules govern the per-series delta:

- **Counter reset.** A replica restart mid-window shows as end < start. The delta must use `increase()` semantics — the series' samples inside the window are scanned, and at a reset the pre-reset value is added to the delta — not `end - start` with a floor at zero. Taking the end value on a negative delta (the naive fallback) undercounts the window containing the reset; worse, summing first and differencing afterwards charges a restarted replica's reset to whichever identities happened to share the grouped series. In PromQL this is `increase(<input_series>[<window>m] @ <end_unix>)` (which also self-anchors, removing the need for the separate start query on that path); if the `last_over_time` pair is kept for consistency with `queryCounterDelta`, the reset correction must be applied in the decoder, per producer series.
- **Missing start anchor (scrape gap).** A series with samples in the window but none in the start lookback has an unknown baseline, not a baseline of zero: treating it as new counts tokens from before the window. Such series are excluded from the window's totals, counted in a debug metric (`llm_dimension_unanchored_series`), and the exclusion is noted in the collection log. A series that stopped being scraped mid-window keeps its last in-window value as the end anchor; its gap is reported the same way.

Engine and attribution series are scraped independently, so the two sides of the coverage check (§1.5) can see slightly different window boundaries. The coverage ratios therefore compare deltas over the *same* `[start, end]` on both sides and are interpreted with a tolerance (§1.9), not as exact equality.

**User label absent.** When the user label is not configured, `<user>` is omitted from the `sum by` and the key's `UserID` is `""`. When it is configured but a series lacks it, Prometheus returns an empty label value and the key is built with `UserID = ""`. Both cases are handled without a separate query.

**Deployment namespace label.** In Prometheus, the `namespace` label on a scraped series is commonly the namespace of the *scrape target*. For a gateway-hosted metering service this would be the gateway's namespace, not the namespace of the model deployment. The requirements document makes the deployment namespace a MUST (F8) for this reason. If the producer emits it under a different label such as `deployment_namespace`, set `INFERENCE_ATTRIBUTION_NAMESPACE_LABEL`. This must be documented prominently because the failure is silent: costs join to the wrong namespace or to nothing.

If `INFERENCE_ATTRIBUTION_ENABLED` is false, `QueryInferenceDimensionTokens` returns an empty result immediately without querying Prometheus.

### 1.4 `InferenceCost` type extensions

Add three fields to each struct, and update `newInferenceCostResponse()` to copy them:

```go
// pkg/inferencecost/types.go: InferenceCostProperties
// Empty when not broken down by dimension, or when user_id is not available (UserID only).
UserID     string
TenantID   string
WorkloadID string

// pkg/inferencecost/apitypes.go: InferenceCostAPIProperties
UserID     string `json:"user_id,omitempty"`
TenantID   string `json:"tenant_id,omitempty"`
WorkloadID string `json:"workload_id,omitempty"`
```

### 1.5 Collector changes (`pkg/inferencecost/collector.go`)

**Call ordering.** `BuildDimensionCosts()` depends on the per-million rates computed by the calculator. The calculator runs in `runner.go`, not inside `CollectMetrics`, so `BuildDimensionCosts()` is also called from `runner.go`:

```
// in runner.go runOnce():
1. modelCosts, dimTokens, err := collector.CollectMetrics()  // model-level slice plus this collection's InferenceDimensionResult
2. calculator.CalculateCosts(modelCosts)                     // populates InputCostPerMillionTokens, OutputCostPerMillionTokens
3. dimCosts := collector.BuildDimensionCosts(modelCosts, dimTokens) // dimension-level []*InferenceCost
4. exporter.Export(modelCosts, dimCosts)                     // exports both slices
```

Moving step 3 inside `CollectMetrics` would require the calculator to run inside the collector, creating a circular dependency.

**`BuildDimensionCosts()` join formula**:

```go
// For each (user_id, tenant_id, workload_id, serving_model, namespace) in dimensionTokens:
//   look up the model entry by serving_model:namespace
//   apply the join independently per cost basis:

AllocationTotalCost = promptTokens     x (InputCostPerMillionTokens[allocation]  / 1_000_000)
                    + completionTokens x (OutputCostPerMillionTokens[allocation] / 1_000_000)

UsageTotalCost      = promptTokens     x (InputCostPerMillionTokens[usage]  / 1_000_000)
                    + completionTokens x (OutputCostPerMillionTokens[usage] / 1_000_000)
```

The model-entry lookup is **not** a plain `serving_model:namespace` map read. It reuses the canonicalized engine-name-to-allocation mapping the model-level join already builds (`combineMetrics` in `collector.go`), which reconciles `org/model` to an allocation-backed `model` key. A producer satisfying F7 can still emit `org/model`; an exact-key lookup would drop its cost as unmatched. The same mapping is reused by the coverage check, so a key joins once or not at all, consistently.

Cached prompt tokens are priced at the full input rate (requirements document, Section 1): OpenCost's rates come from its own allocation, not from a provider's price list, so tenant rows plus the remainder still reconcile exactly. Where a provider discounts cache reads, a tenant's row is an upper bound; that is scoped in the requirements document, not worked around here.

Token counts are set directly from `InferenceDimensionTokens`. All other `Properties` fields (namespace, cluster, model name and so on) are copied from the matched model entry. `AllocationMethod` is inherited.

**Split costs and rates.** Each dimension entry also populates the fields the API response and the aggregation layer recompute from: `InputCost` and `OutputCost` (the entry's cost split into its prompt and completion parts), and `InputCostPerMillionTokens` / `OutputCostPerMillionTokens` copied from the matched model entry. `newInferenceCostResponse` reads these fields directly, and aggregation recomputes differentiated rates from the split costs; a dimension entry with only the two totals set would report a positive `totalCost` with zero input and output cost. The model-level split is not re-run per dimension entry — the entry's split follows from its own token counts and the model's rates.

**Unmatched keys.** A key whose `serving_model:namespace` has no model entry (a model OpenCost does not track, or a namespace mislabel) is not dropped silently. It is counted, logged once per collection cycle with the key, and exposed through a counter metric (`llm_dimension_unmatched_keys`, §1.6) so a label-mapping error is visible. A per-cycle log line is a quiet failure mode for a billing path: the recommended deployment alert is `llm_dimension_unmatched_keys > 0`, and the configuration documentation should say so. The same applies to a non-joining namespace label (§1.3.2): the failure is silent in the cost data, so the metric is the alarm.

**Remainder entry.** After the join, emit one more dimension entry per `(serving_model, namespace)` and cost basis, holding the cost that no attributed series claimed:

```go
// remainder[basis] = max(0, modelTotalCost[basis] - sum(attributed cost[basis]))
// tenant_id = "__unmetered__"; workload_id and user_id empty; tokens = engine tokens - attributed tokens
```

Tenant entries plus the remainder then add up to the model's cost under each basis. Four points:

- *What it contains.* The remainder is defined arithmetically — `max(0, modelTotalCost - attributedCost)` per basis — and described as residual infrastructure cost, not as a formula in coverage. In the common case it approximates the cost of requests that never reached the metering layer, because the per-million rates divide the model's cost across all of the engine's tokens. It is *not* exactly `(1 - coverage) x model cost`: input and completion attribution can differ (full prompt coverage with missing completion usage still leaves a remainder), so the residual is based on the attributed *cost*, which reflects both directions.
- *Zero-token windows.* A model with no engine tokens in the window retains its allocation cost while attributed cost is zero, so the remainder is the model's entire cost for that window — idle included, even when nothing bypassed metering. This case is defined explicitly rather than left to the formula, and it is why the remainder must not be read as "bypass cost" alone.
- *Models with no attributed series.* A model that has engine tokens but no attributed series at all is entirely remainder. That makes models sitting outside the gateway visible.
- *Negative remainders.* If attributed tokens exceed engine tokens (double counting in the producer, or window skew), the remainder is clamped to zero and the over-attribution check warns (§1.5, coverage).

`__unmetered__` is a reserved value, following the `__idle__` and `__unallocated__` convention in OpenCost allocation. It is distinct from the value a producer records for metered requests with missing identity (requirements document §6 and §9). Those requests are in the attributed totals under whatever `tenant_id` the producer uses, for example `unattributed`.

**`CollectMetrics` return type** becomes `([]*InferenceCost, *InferenceDimensionResult, error)`, and `BuildDimensionCosts(modelCosts, dimTokens)` takes the dimension result as an explicit parameter. The result is deliberately **not** stored on `Collector`: the collector is a shared instance (`pkg/cmd/costmodel/costmodel.go`), used concurrently by the background runner and by API-request `computeStep` calls, so a stored field could pair one collection's token counts with another collection's rates — a data race in the cost path that no per-field locking would fix, since the pairing itself must be preserved. Passing the result explicitly keeps each collection's tokens joined to its own rates. The signature change touches `runner.go`, `queryservice.go` and the test mocks once, which is the cheaper trade. `queryservice.go`'s `computeStep` obtains both return values from its own `CollectMetrics` call and passes them to `BuildDimensionCosts` after the local calculator run.

**Coverage checks** (requirements document §8, item 4). After building dimension costs, compute three ratios per `serving_model:namespace`, each logged on breach and exported as its own gauge (`llm_dimension_token_coverage`, `llm_dimension_identity_coverage`):

```go
// 1. Token (metering) coverage, input and output separately:
//    sum(attributed input tokens[m:ns])   / engine prompt tokens delta[m:ns]
//    sum(attributed output tokens[m:ns])  / engine completion tokens delta[m:ns]
//    warn when < threshold (default 0.9, §1.9)
// 2. Over-attribution, input and output separately:
//    warn when the same ratio > 1 + tolerance. This is a separate upper-bound check:
//    the low-coverage warning (< threshold) can never fire for a ratio above one,
//    yet that is exactly the case where tenant rows exceed the model's cost and the
//    remainder is clamped.
// 3. Identity coverage:
//    attributed tokens with non-empty tenant_id and workload_id / all attributed tokens
//    warn when < threshold. Token coverage can read 100% while every request lacks
//    identity (requirements §6 keeps such requests in the counters), so the two are
//    reported separately. user_id is optional and excluded.
```

**Engine reference totals.** The reference for token coverage is defined per deployment (`serving_model`, namespace), not as a global sum over engines. Within one engine, SGLang prefill/decode disaggregation reports the full prompt on both workers, so the reference excludes `engine_type="decode"` (and prefill workers for completion tokens); the engine-side queries added for SGLang support (opencost/opencost#4161) already apply this filter, so the coverage check reuses them rather than adding a second query. But `engine_type` only removes an engine's *own* decode workers: a deployment that disaggregates across engines (SGLang prefill with vLLM decode is a real llm-d topology) is counted twice by a sum over engines. Where a deployment mixes engines, `INFERENCE_ATTRIBUTION_REFERENCE_ENGINE` (§1.9) names the engine whose counters are authoritative for that deployment; unset, OpenCost warns and uses the larger of the per-engine totals rather than the sum.

The engine's own metrics stay independent of the attribution series, which is why these comparisons are meaningful. Both sides use the reset-aware, per-producer deltas of §1.3.2 over the same `[start, end]`, with a tolerance for scrape-boundary skew.

### 1.6 Exporter (`pkg/inferencecost/exporter.go`)

Add the `dimensionCost` gauge and update `Export()` to accept and iterate the dimension-level slice:

```go
dimensionCost = prometheus.NewGaugeVec(
    prometheus.GaugeOpts{
        Name: "llm_dimension_hourly_cost",
        Help: "Hourly infrastructure cost attributed to an inference request dimension (tenant, workload, user).",
    },
    []string{"model_name", "namespace", "cost_basis", "tenant_id", "workload_id", "user_id"},
)

// Number of attributed series in the last collection with no matching model entry.
unmatchedKeys = prometheus.NewGauge(prometheus.GaugeOpts{
    Name: "llm_dimension_unmatched_keys",
    Help: "Attributed token series that could not be joined to a model entry.",
})

// Coverage ratios from the last collection, per model and namespace
// (identity coverage per model and namespace only; see §1.5).
tokenCoverage = prometheus.NewGaugeVec(
    prometheus.GaugeOpts{
        Name: "llm_dimension_token_coverage",
        Help: "Attributed tokens divided by the engine's reference tokens, input and output, per model and namespace.",
    },
    []string{"model_name", "namespace", "direction"},
)
identityCoverage = prometheus.NewGaugeVec(
    prometheus.GaugeOpts{
        Name: "llm_dimension_identity_coverage",
        Help: "Share of attributed tokens carrying non-empty tenant_id and workload_id, per model and namespace.",
    },
    []string{"model_name", "namespace"},
)

// Debug: series excluded from a window because no start anchor exists (§1.3.2).
unanchoredSeries = prometheus.NewGauge(prometheus.GaugeOpts{
    Name: "llm_dimension_unanchored_series",
    Help: "Attributed token series excluded from the window: no sample at the start anchor (scrape gap).",
})
```

**Estimated tokens** (requirements document §7.3). When the producer separates estimated counts into sibling series (for example `..._estimated_total`), their optionally-configured series names are queried with the same delta strategy, added to the attributed totals, and exported as `llm_dimension_estimated_token_share` (estimated tokens / attributed tokens, per model and namespace), so a consumer can tell an estimate from an exact count. The `estimated` label variant needs no extra configuration: the labelled series are summed into the attributed totals and into the estimated share by the decoder.

When `user_id` is empty, the gauge is still emitted with an empty label value. This is valid Prometheus behaviour and preserves `tenant_id` and `workload_id` attribution.

### 1.7 Aggregation and API

**`pkg/inferencecost/aggregate.go`**: add three new dimensions to `supportedAggregateProperties`, three new `case` arms in `aggKey()` and `matchesFilter()`, and three new property-clear guards in `aggregate()`.

**Aggregation keys for dimension entries.** `aggKey()` for a dimension entry must include the dimension fields themselves, not only model and namespace: `computeStep` inserts entries into a map keyed by `aggKey`, so a dimension entry keyed by model and namespace alone overwrites different tenants' entries before aggregation ever runs. The key encoding must also be unambiguous. The existing aggregation joins values with `/`, and dimension values may legitimately contain `/` (tenant ids like `acme-corp/engineering` do), so a `/`-joined key collides `(acme-corp/engineering, rag)` with `(acme-corp, engineering/rag)`. Dimension-aware keys use a separator that cannot appear in the values, or escape (or length-prefix) each component; the same rule applies to the natural key `serving_model:namespace` wherever it is built as a string.

**`pkg/inferencecost/queryservice.go`, `computeStep()`**: take both return values from its own `collector.CollectMetrics()` call and pass them to `collector.BuildDimensionCosts(modelCosts, dimTokens)` immediately after the local `CalculateCosts` call, then include dimension-level entries in the `InferenceCostSet`. Their properties carry all three new fields plus the full model, namespace and cluster properties, so existing aggregation by `model_name` or `namespace` still sums correctly across tenants, workloads and users.

> **Double-counting caution.** Model-level and dimension-level entries both live in the `InferenceCostSet`. An aggregation that includes both would count cost twice. Before implementation, decide whether dimension entries are returned only when an attribution dimension is requested in `aggregate` or `filter`, and test that `aggregate=model_name` with attribution enabled equals the model-level total. The remainder entry (§1.5) helps here: dimension entries, including the remainder, partition the model's cost, so summing only the dimension entries also equals the model-level total.

**`pkg/inferencecost/queryservice_helper.go`**: update validation error messages to list the three new dimensions.

### 1.8 File change summary

| File | Change | What |
|---|---|---|
| `core/pkg/source/inference_results.go` | Add | `InferenceDimensionKey`, `InferenceDimensionTokens`, `InferenceDimensionResult` |
| `core/pkg/source/datasource.go` | Extend | `QueryInferenceDimensionTokens` constant and interface method |
| `core/pkg/source/decoders.go` | Add | `DecodeInferenceDimensionResult` |
| `core/pkg/source/noop.go`, `record.go`, `mock.go` | Add | Implementations for `QueryInferenceDimensionTokens` (mock supports override injection) |
| `modules/prometheus-source/pkg/prom/inference_queries.go` | Add | `QueryInferenceDimensionTokens`, `queryDimensionCounterDelta`, decoder, `mergeDimensionDeltas`; PromQL built from configured series and label names |
| `modules/collector-source/pkg/collector/metricsquerier.go` | Add | Stub (returns empty) |
| `pkg/env/costmodel.go` | Extend | Getters for the `INFERENCE_ATTRIBUTION_*` variables (§1.9) |
| `pkg/inferencecost/env.go` | Extend | Thin readers for the new variables |
| `pkg/inferencecost/types.go` | Extend | `UserID/TenantID/WorkloadID` on `InferenceCostProperties`; attribution fields on `Config` |
| `pkg/inferencecost/apitypes.go` | Extend | `UserID/TenantID/WorkloadID` on `InferenceCostAPIProperties`; update `newInferenceCostResponse` |
| `pkg/inferencecost/collector.go` | Extend | Dimension-token future; `CollectMetrics` returns the dimension result; `BuildDimensionCosts(modelCosts, dimTokens)` with canonicalized model lookup, split costs and remainder; unmatched-key handling; coverage, over-attribution and identity-coverage checks |
| `pkg/inferencecost/aggregate.go` | Extend | Three new dimensions; dimension-aware, unambiguous `aggKey` encoding |
| `pkg/inferencecost/exporter.go` | Extend | `llm_dimension_hourly_cost`, `llm_dimension_unmatched_keys`, `llm_dimension_token_coverage`, `llm_dimension_identity_coverage`; updated `Export` signature |
| `pkg/inferencecost/runner.go` | Extend | Accept the dimension result from `CollectMetrics`; call `BuildDimensionCosts(modelCosts, dimTokens)` after `CalculateCosts()`; pass both slices to `Export()` |
| `pkg/inferencecost/queryservice.go` | Extend | Same `CollectMetrics`/`BuildDimensionCosts` pairing as `runner.go`; `user_id` aggregate/filter default-deny (§1.7 authorization); add dimension entries (see §1.7 caution) |
| `pkg/inferencecost/queryservice_helper.go` | Extend | Updated error messages |
| `docs/inference-cost-tracking.md` | Extend | New env vars; link to the requirements document |

### 1.9 Configuration

New environment variables, read through `pkg/env/costmodel.go` like the existing `INFERENCE_*` variables:

| Variable | Default | Purpose |
|---|---|---|
| `INFERENCE_ATTRIBUTION_ENABLED` | `false` | Opt in to querying attribution series and computing dimension costs. When false, model-level costs are unaffected. |
| `INFERENCE_ATTRIBUTION_INPUT_TOKENS_SERIES` | none (required when enabled) | Full name of the attributed input-token series (counter or histogram `_sum`). |
| `INFERENCE_ATTRIBUTION_OUTPUT_TOKENS_SERIES` | none (required when enabled) | Full name of the attributed output-token series. |
| `INFERENCE_ATTRIBUTION_INPUT_TOKENS_ESTIMATED_SERIES` | none | Sibling series holding estimated input tokens (requirements §7.3). Optional; when set, its deltas are added to attributed totals and exported as the estimated share (§1.6). |
| `INFERENCE_ATTRIBUTION_OUTPUT_TOKENS_ESTIMATED_SERIES` | none | Sibling series holding estimated output tokens. As above. |
| `INFERENCE_ATTRIBUTION_TENANT_LABEL` | `tenant_id` | Label carrying the tenant. |
| `INFERENCE_ATTRIBUTION_WORKLOAD_LABEL` | `workload_id` | Label carrying the workload. |
| `INFERENCE_ATTRIBUTION_USER_LABEL` | `user_id` | Label carrying the user. Set to empty to disable user-level grouping. |
| `INFERENCE_ATTRIBUTION_MODEL_LABEL` | `serving_model` | Label carrying the **served** model. Must equal the engine's `model_name`. |
| `INFERENCE_ATTRIBUTION_NAMESPACE_LABEL` | `namespace` | Label carrying the **deployment** namespace (see §1.3.2). |
| `INFERENCE_ATTRIBUTION_COVERAGE_THRESHOLD` | `0.9` | Ratio below which the token-coverage and identity-coverage warnings are logged. |
| `INFERENCE_ATTRIBUTION_OVERATTRIBUTION_TOLERANCE` | `0.1` | Amount above 1.0 at which the over-attribution warning is logged (ratio > 1 + tolerance). |
| `INFERENCE_ATTRIBUTION_REFERENCE_ENGINE` | none | For deployments that disaggregate prefill/decode **across engines**: the engine whose token counters are authoritative for the coverage reference (`vllm` or `sglang`). Unset, engines are summed per deployment, which double counts cross-engine disaggregation; OpenCost warns and falls back to the larger per-engine total when it detects both engines reporting the same deployment. |
| `INFERENCE_ATTRIBUTION_USER_AGGREGATION_ENABLED` | `false` | Allows `aggregate`/`filter` on `user_id` in the API, and only together with admin authentication (`ADMIN_TOKEN`). Default-deny otherwise (§1.7 authorization). The `user_id` label on the exported metric is governed by `INFERENCE_ATTRIBUTION_USER_LABEL` and Prometheus RBAC, not by this flag. |

The series names have no default on purpose: any default would tie OpenCost to one producer, which is what this plan avoids. If the series names are unset while attribution is enabled, OpenCost logs an error at startup and disables attribution. It does not fail model-level collection.

Example for the llm-d coordinator as producer (metric names from its attribution proposal):

```yaml
env:
  - name: INFERENCE_ATTRIBUTION_ENABLED
    value: "true"
  - name: INFERENCE_ATTRIBUTION_INPUT_TOKENS_SERIES
    value: "llm_d_coordinator_request_input_tokens_attributed_total"
  - name: INFERENCE_ATTRIBUTION_OUTPUT_TOKENS_SERIES
    value: "llm_d_coordinator_request_output_tokens_attributed_total"
  - name: INFERENCE_ATTRIBUTION_MODEL_LABEL
    value: "model_name"
```

Moving the producer later, for example from the coordinator to an AI gateway, is a configuration change.

### 1.10 Tests

| Test file | What to add |
|---|---|
| `core/pkg/source/decoders_test.go` | `TestDecodeInferenceDimensionResult`, including `user_id` absent |
| `pkg/inferencecost/collector_test.go` | Join formula; zero model cost (no panic); empty attribution result; missing user label; **unmatched key counted and logged, not dropped silently**; coverage, over-attribution and identity-coverage warnings; **remainder entry: tenant rows plus remainder equal the model total under both bases, a model with no attributed series is all remainder, an over-attributed model clamps to zero, and a zero-token window puts the model's full cost in the remainder**; **split costs and per-million rates populated on dimension entries (positive input and output cost, not just totals)**; **canonicalized model lookup: a producer emitting `org/model` joins to the `model` allocation entry** |
| `pkg/inferencecost/aggregate_test.go` | Aggregation by each new dimension; filter by `tenant_id:"acme-corp"`; empty `user_id`; **model-level aggregation with attribution enabled equals the model-level total (no double counting)**; **`aggKey` collision: `(acme-corp/engineering, rag)` and `(acme-corp, engineering/rag)` remain distinct entries**; **`user_id` aggregate/filter rejected without the enablement flag and without admin auth** |
| `pkg/inferencecost/exporter_test.go` | `llm_dimension_hourly_cost` emitted correctly; empty `user_id` as empty label; `__unmetered__` row emitted; `llm_dimension_unmatched_keys`; `llm_dimension_token_coverage` and `llm_dimension_identity_coverage` |
| `pkg/inferencecost/env_test.go` or equivalent | Defaults; missing series names disables attribution with an error and leaves model-level collection running |
| `modules/prometheus-source/pkg/prom/inference_queries_test.go` | PromQL generated from custom label and series names; user label disabled; one token series absent (graceful degradation); **counter reset mid-window, per producer series (increase semantics, not end-value fallback)**; **multi-replica reset: one replica resets while another keeps counting, deltas stay correct per series**; **scrape gap: series with no start anchor excluded and counted, not treated as starting from zero; series that stopped mid-window keeps its last value**; **boundary skew: engine and attribution series scraped at different offsets stay within tolerance** |

---

## 2. Key design decisions

| Decision | Chosen approach | Rationale |
|---|---|---|
| **Producer coupling** | Series and label names are configuration; no producer-specific discovery | The attribution producer has already moved between components, and may move again. OpenCost should not change when it does. Contract: the requirements document §7.3. |
| **Source series type** | Counter (`_total`) or histogram `_sum`, both accepted | Both are monotonically increasing, so the `queryCounterDelta` pattern applies unchanged. |
| **Cost join formula** | `promptTokens x (inputCostPerM / 1M) + completionTokens x (outputCostPerM / 1M)` | Token sums drive cost. Request count is not used, since input and output rates differ. |
| **Calculator call order** | Calculator runs on model entries first; `BuildDimensionCosts` uses the resulting rates | No need to re-run cost-split logic per dimension entry. |
| **`user_id` handling** | Single global label setting; empty when absent | OpenCost does not impose its own cardinality cap. That decision belongs to the producer (requirements document §4.2). |
| **`CollectMetrics` signature** | Extended to return the dimension result; `BuildDimensionCosts` takes it as a parameter | The collector is shared by the background runner and API requests, so a stored result is a data race that pairs one collection's tokens with another's rates. A one-time signature change is cheaper than preserving the pairing incorrectly. |
| **Served-model join key** | `serving_model` joined with deployment namespace, through the canonicalized `combineMetrics` mapping | The served model must equal the engine's `model_name`, so the join is consistent by construction (requirements document F7). The canonicalized mapping keeps producers emitting `org/model` joinable instead of unmatched. |
| **Delta strategy** | Per-producer, reset-aware deltas (`increase()` semantics), then grouped by identity | Summing first and differencing afterwards misattributes replica resets; the end-value fallback undercounts the reset window. |
| **Coverage reference** | Per-deployment engine totals, with `engine_type` exclusions and a configured authoritative engine for cross-engine disaggregation | A global sum over engines double counts deployments that split prefill and decode across engines (SGLang prefill + vLLM decode). |
| **Deployment namespace label** | Separately configurable | The scrape-target `namespace` is wrong for gateway-hosted producers. A silent mismatch is the main operational risk (§1.3.2). |
| **`requested_model` excluded** | Not a group-by label | The cost rate follows the served model. Including the requested model inflates cardinality without adding cost signal. |
| **Unmatched series** | Counted, logged and exported, never dropped silently | Label-mapping errors would otherwise look like zero cost. |
| **Backward compatibility** | New fields empty by default; feature opt-in | Deployments without attribution are unaffected. |

---

## Appendix A: Multiple deployments

A cluster may hold several independent model deployments, each in its own namespace. OpenCost is deployed once per cluster. The `serving_model:namespace` composite key partitions attributed tokens per deployment automatically, provided the deployment namespace label is correct (§1.3.2). No per-namespace OpenCost configuration is needed.

Because the metric and label names are global settings, **all producers in the cluster must use the same series and label names**. A cluster that mixes two producers with different names is not supported by this plan (see open questions).

### Known limitation: shared-infrastructure label is a single global value

`INFERENCE_SHARED_INFRA_LABEL` and `INFERENCE_SHARED_INFRA_LABEL_VALUE` identify shared infrastructure pods whose costs are distributed across model pods. If two deployments use different label schemes for shared infrastructure, they cannot be expressed per namespace. This pre-dates this plan. The severity is low while llm-d standardises `llm-d.ai/inference-shared=true`.

### What works without changes

- Model-level cost collection: the token queries key by `(model_name, namespace)`, but today the allocation query aggregates by the model label alone. See the prerequisite below.
- `BuildDimensionCosts()`: keys on `serving_model:namespace`, so costs go to the correct deployment, once the prerequisite below is in place.
- Coverage check: runs per `serving_model:namespace`, each deployment independently.
- API filtering: `?filter=namespace:"llm-d-prod"` isolates one deployment.

### Prerequisite: model-level cost must be keyed by namespace

The dimension join looks up the model entry by `serving_model:namespace`. Today the collector aggregates allocations by the model label alone (`label:<INFERENCE_MODEL_LABEL>`). When the same label value is used in two namespaces, the allocations merge into one entry with an empty namespace, so no namespace matches its cost and each reports zero ([opencost/opencost#4162](https://github.com/opencost/opencost/issues/4162)). Every dimension entry for such a model would then get zero cost, and would also be counted as unmatched.

[opencost/opencost#4163](https://github.com/opencost/opencost/pull/4163) fixes this by aggregating by the model label and namespace. At the time of writing it is open and unmerged. This plan depends on it, and the dimension join builds directly on that join. Multi-namespace deployments of one model, which is the case requirements document §8 step 1.2 handles, are only correct once it lands. Add a test to this plan's suite that covers the same model in two namespaces end to end, through to the dimension entries.

---

## Appendix B: Open questions

- **Mixed producers.** Whether to support a list of (series, label mapping) sets, so a cluster with a coordinator-based and a gateway-based deployment can both be queried. This plan supports one set.
- **Gateway-side cost.** If the metering service computes cost itself, there are two sources of truth for self-hosted model rates. The requirements document §11 leaves ownership open. This plan assumes OpenCost owns the rate.
- **Dimension entries in the cost set.** The double-counting decision in §1.7.
- **Idle as its own row.** Today the idle share is inside the per-million rate, so tenants with traffic carry it in proportion to their tokens, and the `__unmetered__` row holds unmetered requests (and, in zero-token windows, the model's retained cost — §1.5). Showing idle separately would need tenant rows priced without idle, with idle in the remainder. The two existing cost queries do not give pure idle: the allocation total is `max(request, usage) x price` plus idle share plus shared-infrastructure share, and the usage total is actual consumption only (`inference-cost-tracking.md`). Their difference mixes idle, request headroom and shared infrastructure. A separate idle row would need a dedicated idle query, and it would change what `cost_basis=allocation` means for tenant rows. Not part of this plan.
