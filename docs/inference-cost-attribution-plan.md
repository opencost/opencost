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

**One rate source.** OpenCost owns the rate for self-hosted models, so a metering layer's own price list or credit figures are never combined with it, and the serving stack supplies identity and tokens only (requirements §1, Rate ownership).

**Self-hosted models only.** Attribution allocates cluster cost (GPU, idle and shared infrastructure), so it covers self-hosted models by construction. A SaaS model (OpenAI, Anthropic, Vertex and similar) has no cluster cost to allocate, and its price is already per-token billing data. OpenCost does reach some SaaS spend through the custom-cost plugins, for example the OpenAI plugin in `opencost-plugins`, which reports organization, project and model. That is a separate pipeline: it is not part of `/inferenceCost` or `llm_dimension_hourly_cost`, and it is not broken down by `tenant_id`, `workload_id` or `user_id`. As a result, `tenant_id` totals from attribution will **not** equal total AI spend, and a chargeback consumer must not read them that way. Mapping provider projects or keys to tenants, so that SaaS spend could be attributed too, is a possible bridge but out of scope here.

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

**Idle-cost policy.** Under `cost_basis=allocation`, the model's cost includes idle GPU capacity, and the per-million rates spread that idle across the dimension rows in proportion to token counts: a tenant that drove 40% of a model's tokens pays 40% of its idle. This proportional spread is a **stated chargeback policy, not a neutral default**: tenants with traffic subsidize idle capacity, and a tenant that ramps up later inherits idle accumulated while it was not calling. Operators who want a different policy — idle absorbed by the platform, split evenly per tenant, or reported separately — cannot get it from this plan (see Appendix B). `cost_basis=usage` carries no idle and answers "cost of my requests" directly; consumers who need that distinction should read the `usage` rows alongside the `allocation` rows.

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

**Authorization for `user_id`.** Per-person spend is sensitive, and `/inferenceCost` has no tenancy model of its own. The gate is on what the response would **expose**, not on what the query mentions: per-user entries appear in responses that never name `user_id` — `?filter=tenant_id:"acme"` with no aggregation returns entries unchanged, each still carrying `user_id` and per-user cost — so restricting only explicit `user_id` queries leaks the same data through any filter. Three rules:

1. A request with `aggregate` or `filter` on `user_id` requires both `INFERENCE_ATTRIBUTION_USER_AGGREGATION_ENABLED=true` (§1.9, default false) and admin authentication (the existing `ADMIN_TOKEN` bearer mechanism). Anything less receives a validation error naming the restriction.
2. Any other request whose response would contain entries with a non-empty `user_id` — filter-only, or no aggregation at all — is answered for unauthorized callers by **merging those entries across `user_id`** (cost and tokens summed, `user_id` stripped from properties and map keys) rather than by erroring. Tenant- and workload-level queries keep working; per-user detail does not leak.
3. `tenant_id` and `workload_id` are not restricted.

The same applies on the metrics path: the `user_id` label on `llm_dimension_hourly_cost` is readable by anyone with Prometheus query access, so a deployment exposing per-user cost must restrict the metric with Prometheus RBAC or disable the user label (§1.9). Hiding per-user dollars in a UI is moot if the API and the metric still return them.

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
// InferenceDimensionKey identifies a (producer, user_id, tenant_id, workload_id, serving_model, namespace)
// combination. Producer is part of the key so that per-producer checks survive until they have run (§1.3.1). UserID is empty when the user label is not configured or absent from the series.
type InferenceDimensionKey struct {
    Producer     string // name of the producer mapping that emitted the series (§1.9); "default" for a single env-var mapping
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

    // Estimated subtotals (requirements §7.3): tokens the producer flagged as
    // estimates rather than exact counts. Subsets of the totals above, carried
    // separately so the exporter can compute the estimated share (§1.6).
    EstimatedPromptTokens     float64
    EstimatedCompletionTokens float64
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

There is no producer-discovery query. Configuration holds one mapping (env vars) or a list of mappings (config file, §1.9); with several mappings the query method runs once per mapping and merges the results into one `InferenceDimensionResult`. The user label is configured per mapping, and its absence from a series is handled by the query (§1.3).

Stubs required in: `core/pkg/source/noop.go`, `record.go`, `mock.go`, and `modules/collector-source/pkg/collector/metricsquerier.go`.
Decoder required in: `core/pkg/source/decoders.go` (`DecodeInferenceDimensionResult`).

### 1.3 Prometheus queries (`modules/prometheus-source/pkg/prom/inference_queries.go`)

#### 1.3.1 Configurable series

OpenCost does not know the producer's metric names. They come from configuration (§1.9): a single mapping through environment variables, or a **list of mappings** through a config file, so a cluster running more than one producer — a gateway in front of the llm-d coordinator, say — can be queried in full. Each mapping specifies, independently:

- Input-token series and output-token series. Each may be a counter (`..._total`) or a histogram `_sum` series. Both behave as monotonically increasing counters, so the same delta pattern works for either. The configured value is the full series name.
- Optional estimated-token series (input and output), for producers that separate estimated counts per the requirements document §7.3 (sibling-series variant). Estimated tokens count as attributed and are exported as a separate share (§1.6).
- Label names for tenant, workload, user, serving model and deployment namespace, and the producer's missing-identity sentinel (`identitySentinel`, and `userSentinel` for the user label). The workload and user labels may be set to empty when the producer has no such label; that disables grouping by it for the mapping, and drops it from the identity-coverage check (§1.5). Defaults match the requirements document: `tenant_id`, `workload_id`, `user_id`, `serving_model`, `deployment_namespace`, `unattributed`. The namespace default is deliberately not `namespace` (§1.3.2).

With several mappings configured, the delta queries of §1.3.2 run once per mapping and all results go into one `InferenceDimensionResult`. **Each entry keeps its `Producer` in the key** until the per-producer checks have run: unmatched-key counts, identity coverage (each mapping with its own missing-identity sentinel) and the namespace-resolution step all need to know which producer contributed a value. `none` can be a real tenant for one producer and the sentinel for another, and merged totals cannot tell the two apart. Only afterwards, in `BuildDimensionCosts`, are entries with identical identity values (`user_id`, `tenant_id`, `workload_id`, `serving_model`, namespace) summed into one row, so one tenant served through two producers is one row. Token-coverage, remainder and over-attribution accounting then run over the merged union. A producer whose mapping is missing or misconfigured shows up as that producer's traffic in the remainder — never as another producer's traffic — which is why each mapping also labels the unmatched-keys metric (§1.6).

**Producers must meter disjoint traffic.** Summing producers is only correct when no request is metered twice. A gateway in front of the llm-d coordinator can see the same requests as the coordinator, and merging identical keys does not deduplicate observations; clamping the remainder afterwards cannot undo the excess charges. Where two producers sit on the same request path, exactly one is the authoritative producer for that path, and the other is restricted away from it with the mapping's `scope` (§1.9: `servingModels` and/or `namespaces`; empty means all). Rows a mapping emits outside its scope are dropped before the join. OpenCost cannot prove disjointness from the series alone, since two producers legitimately meter different ingress paths of the same deployment, so it detects the failure instead: when two or more producers emit rows for the same `(serving_model, namespace)`, the over-attribution check (§1.5) reports the producers involved, and it fires on the summed ratio.

The generated PromQL substitutes configured names. In the examples below, `<tenant>`, `<workload>`, `<user>`, `<model>` and `<ns>` stand for the configured label names, and `<input_series>` for the configured input series.

#### 1.3.2 `QueryInferenceDimensionTokens`: counter delta strategy

Two queries in total (input and output), each a per-producer, reset-aware range query:

```promql
# Input tokens over the window. increase() evaluates per original series
# (job, instance are retained inside the function); the outer sum groups by
# identity only after the per-series delta has been computed.
sum by (<user>, <tenant>, <workload>, <model>, <ns>) (
  increase(<input_series>[<window>s] @ <end_unix>)
)
```

`<window>s` = `windowDuration.Seconds()`: the **exact** requested interval, ending at `effectiveEnd`. The range is never widened to obtain more samples. A longer range would pull in the preceding interval's traffic and charge a tenant that made no requests in the window asked for. The cost of that choice is that a window shorter than about two scrape intervals can hold fewer than two samples per series, in which case `increase()` returns nothing for them (see Scrape gaps below). Queries are issued at `effectiveEnd` (clamped to `time.Now()`) to avoid future-timestamp errors. The same pattern applies to the output series. The helper `queryDimensionCounterDelta` encapsulates the query and is called twice (input, output). Results are merged into one `InferenceDimensionResult`.

This deliberately departs from the `last_over_time` end/start pair that `queryCounterDelta` uses. Two endpoint samples cannot account for counter resets, in either direction:

- **Counter reset.** A replica restart mid-window can show as end < start, but it can also hide behind a growing counter: start 100, reset to 0, end 300 reads as +200 from the endpoints when the true increase is 300. `increase()` scans the samples inside the range per series and adds the pre-reset value at a reset, which is why the delta MUST be computed by a range function that sees the intervening samples. The naive fallbacks — substituting the end value on a negative delta, or applying a reset correction in the decoder to two `last_over_time` results — cannot see those samples and MUST NOT be used. `increase()` also self-anchors, so no separate start-of-window query is needed on this path.
- **Scrape gaps and new series.** An interior gap in a series' samples loses nothing, because counters are cumulative and `increase()` spans the gap. The limits are at the edges of a series' life:
  - A series with a **single sample** in the window is omitted by `increase()`.
  - A series whose **first scrape already reads a non-zero value** (for example 100, then 100 again) yields an increase of zero: the increments before the first scrape are invisible. A tenant that makes one short burst of requests, shorter than the window and created between two scrapes, can therefore be missing from the tenant rows entirely.
  - A series that stops being scraped mid-window is counted up to its last sample.

  None of these is treated as starting from zero, and none can charge another identity. Their tokens still appear in the engine reference, so they land in the remainder (`__unmetered__`) and persistent loss shows as low token coverage (§1.5) rather than as misattributed cost. Producers mitigate the new-series case by exporting the series at zero when the identity is first known (requirements §7.3). This is a documented limitation of the approach, not a guarantee of complete accounting for newly appearing series.

Grouping by identity happens only after the per-series deltas are computed. Summing first and differencing afterwards is forbidden: it charges a restarted replica's reset to whichever identities happened to share the grouped series.

**Estimated tokens.** The sibling estimated series (§1.9) are queried the same way, into separate subtotals on the result (§1.1); the `estimated`-label variant sums the labelled series separately within the same query. The classification must survive to the exporter, which computes the estimated share from the subtotals (§1.6). Merging estimated and exact counts into a single number before the share is computed would make that promise unimplementable.

Engine and attribution series are scraped independently, so the two sides of the coverage check (§1.5) can see slightly different window boundaries. The coverage ratios therefore compare deltas over the same `[start, end]` on both sides and are interpreted with a tolerance (§1.9), not as exact equality.

**User label absent.** When the user label is not configured, `<user>` is omitted from the `sum by` and the key's `UserID` is `""`. When it is configured but a series lacks it, Prometheus returns an empty label value and the key is built with `UserID = ""`. Both cases are handled without a separate query.

**Deployment namespace label.** In Prometheus, the `namespace` label on a scraped series is usually the namespace of the *scrape target* (the relabeling that Prometheus Operator and most scrape configs apply). For a gateway-hosted metering service that is the gateway's namespace, not the namespace of the model deployment, so defaulting to `namespace` would join every key to the wrong namespace without any error. The default is therefore a **producer-owned** label, `deployment_namespace` (requirements document F8 and §7.3), which a producer sets only when it knows the deployment it routed to.

Three rules follow:

1. **Default `deployment_namespace`.** A series that does not carry it has an empty namespace and goes through the missing-namespace resolution path (§1.5), where only the last step is approximate and it is marked as such. It is never silently joined to the scrape target's namespace.
2. **Using the scrape-time `namespace` is an explicit choice.** `INFERENCE_ATTRIBUTION_NAMESPACE_LABEL=namespace` (or `namespaceLabel: namespace` in a mapping) is correct only when the producer runs in the same namespace as the models it meters, for example a coordinator deployed per model deployment. The operator must set it, so the assumption is written down in the configuration rather than inherited from a default.
3. **A label that does not join is a loud condition, not a log line.** OpenCost validates the mapping on the first collection cycle after startup and re-evaluates it each cycle (the *attribution health check*, §1.5, Unmatched keys). If a producer has attributed series but **none** of its keys joins a model entry, or the namespaces it reports are not namespaces of any tracked model, OpenCost logs at ERROR with the producer name, the configured namespace label and the likely cause, and the condition stays visible in `llm_dimension_unmatched_keys{producer}` until fixed. The recommended deployment alert is `llm_dimension_unmatched_keys > 0` (§1.5).

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
Approximate bool  `json:"approximate,omitempty"` // namespace resolved by proportional split (§1.5), or row cost scaled to reconcile (§1.5, Over-attribution)
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

**Missing namespace resolution.** F8 permits a producer to omit the deployment namespace, and the requirements document (§8, step 1) promises a resolution path — so a key with an empty namespace does not go straight to unmatched. `BuildDimensionCosts` resolves it in that order before the lookup:

1. **Engine metrics.** The engine's own token series carry the scrape-time namespace, keyed by the canonicalized model name. A model serving in exactly one namespace resolves there.
2. **Configuration.** A model in several namespaces resolves through the operator-supplied mapping (`INFERENCE_ATTRIBUTION_NAMESPACE_MAP`, §1.9).
3. **Proportional split.** Still unresolved, the key's tokens are split across the namespaces serving that model in proportion to each namespace's engine token share over the window (the per-deployment reference of §1.5). Each part is priced at **its own namespace's** rates and becomes a row in that namespace, marked approximate — an `approximate` property on the API response (§1.4) and a log line — so estimated cost is not mistaken for reconciled cost. The split parts count as attributed in each namespace, so they are deducted from that namespace's remainder like any other row. This replaces pricing the key at one blended average rate, which produced a charge outside every namespace's remainder and broke reconciliation. If the namespaces have no engine tokens in the window, the key stays unresolved and counts as unmatched.

Only a key still unresolved after step 3 counts as unmatched. Steps 1 and 2 are exact; only step 3 is an approximation, and it is visible as one. Namespace resolution runs per producer entry, before entries of different producers are merged (§1.3.1).

Cached prompt tokens are priced at the full input rate (requirements document, Section 1): OpenCost's rates come from its own allocation, not from a provider's price list, so tenant rows plus the remainder still reconcile exactly. Where a provider discounts cache reads, a tenant's row is an upper bound; that is scoped in the requirements document, not worked around here.

Token counts are set directly from `InferenceDimensionTokens`. All other `Properties` fields (namespace, cluster, model name and so on) are copied from the matched model entry. `AllocationMethod` is inherited.

**Split costs and rates.** Each dimension entry also populates the fields the API response and the aggregation layer recompute from: `InputCost` and `OutputCost` (the entry's cost split into its prompt and completion parts), and `InputCostPerMillionTokens` / `OutputCostPerMillionTokens` copied from the matched model entry. `newInferenceCostResponse` reads these fields directly, and aggregation recomputes differentiated rates from the split costs; a dimension entry with only the two totals set would report a positive `totalCost` with zero input and output cost. The model-level split is not re-run per dimension entry — the entry's split follows from its own token counts and the model's rates.

**Unmatched keys.** A key whose `serving_model:namespace` has no model entry (a model OpenCost does not track, or a namespace mislabel) is not dropped silently. It is counted, logged once per collection cycle with the key, and exposed through a counter metric (`llm_dimension_unmatched_keys`, §1.6) so a label-mapping error is visible. A per-cycle log line is a quiet failure mode for a billing path: the recommended deployment alert is `llm_dimension_unmatched_keys > 0`, and the configuration documentation should say so. The same applies to a non-joining namespace label (§1.3.2): the failure is silent in the cost data, so the metric is the alarm.

**SaaS and other non-cluster models.** A gateway producer can meter traffic to models that OpenCost has no allocation for, for example a SaaS model routed through the same LiteLLM instance. Their series carry a `serving_model` with no model entry, so left alone they count as unmatched, hold `llm_dimension_unmatched_keys > 0` permanently, and teach operators to ignore the alert that is meant to catch label-mapping errors. They are expected and excluded instead, in one of two ways:

1. **Producer side.** The producer does not emit attributed series for models it does not serve on the cluster.
2. **OpenCost side.** The mapping's `scope` (§1.9) names the self-hosted models with `servingModels`, or names the excluded ones with `excludeServingModels`. Rows outside the scope are dropped before the join, are **not** counted as unmatched, and do not enter the token-coverage or health-check calculations, which compare against self-hosted engine counters only.

Without either, SaaS series are indistinguishable from a model mislabel, and are reported as unmatched. Dropped rows are logged once at startup per producer, with the excluded model names, so the exclusion is visible rather than silent.

**Attribution health check.** The metric alone is passive, so a mapping that joins nothing is also reported actively. On the first collection after startup, and on every cycle thereafter, OpenCost evaluates each producer: if it has attributed series but no key joins a model entry (or every namespace it reports is a namespace of no tracked model), it logs at ERROR naming the producer, the configured `namespaceLabel` and the likely cause (a scrape-target namespace, or a label the producer does not emit). The ERROR repeats while the condition holds, rather than being a single INFO line per cycle with the key. It does not stop model-level collection. Partial failure, where some keys join and some do not, is reported by the metric and the WARN log.

**Remainder entry.** After the join, emit one more dimension entry per `(serving_model, namespace)` and cost basis, holding the cost that no attributed series claimed:

```go
// remainder[basis] = max(0, modelTotalCost[basis] - sum(attributed cost[basis]))
// tenant_id = "__unmetered__"; workload_id and user_id empty; tokens = engine tokens - attributed tokens
```

Tenant entries plus the remainder then add up to the model's cost under each basis. Five points:

- *What it contains.* The remainder is defined arithmetically — `max(0, modelTotalCost - attributedCost)` per basis — and described as residual infrastructure cost, not as a formula in coverage. In the common case it approximates the cost of requests that never reached the metering layer, because the per-million rates divide the model's cost across all of the engine's tokens. It is *not* exactly `(1 - coverage) x model cost`: input and completion attribution can differ (full prompt coverage with missing completion usage still leaves a remainder), so the residual is based on the attributed *cost*, which reflects both directions. Idle cost is never in the remainder; it stays spread across the tenant rows in proportion to their tokens — a stated chargeback policy (see New outputs, "Idle-cost policy"), not an arithmetic necessity.
- *Zero-token windows.* A model with no engine tokens in the window retains its allocation cost while attributed cost is zero, so the remainder is the model's entire cost for that window — idle included, even when nothing bypassed metering. This case is defined explicitly rather than left to the formula, and it is why the remainder must not be read as "bypass cost" alone.
- *Models with no attributed series.* A model that has engine tokens but no attributed series at all is entirely remainder. That makes models sitting outside the gateway visible.
- *Reading it as a control.* A persistent non-zero remainder, or low token coverage, usually means a request path reaches the engine without passing the metering layer: a key minted before onboarding completes, a service account that skips the gateway, or a model deployed outside it. The same cost is also the first sign of a gap in key issuance. The recommended operations response is an alert on the `__unmetered__` share and on `llm_dimension_token_coverage` and `llm_dimension_identity_coverage` (§1.6), and closing the path that caused it, not dashboard tuning. A metered request with no tenant mapping is a different case: it appears under the missing-identity sentinel, lowers identity coverage, and leaves token coverage unchanged (requirements §8.1). Where a remainder is expected, exclude it by configuration (§1.5, SaaS and other non-cluster models) so a non-zero row keeps its meaning.
- *Over-attribution.* If attributed cost exceeds the model's cost under a basis (double counting in a producer, or window skew), clamping the remainder to zero is not enough: a $10 model with 100 engine tokens and 105 attributed tokens would show $10.50 in tenant rows and a zero remainder. So the tenant rows themselves are **scaled down pro rata** by `modelTotalCost / attributedCost`, under that basis and for that `(serving_model, namespace)`, and the remainder is then exactly zero. This applies at any excess, including skew inside the tolerance, so the sum of tenant rows plus remainder equals the model's cost unconditionally and the equality release gate (§1.7) holds. Scaled rows are marked `approximate`, and their token counts stay as reported (only cost is scaled). The over-attribution warning (§1.5) is separate: it fires only beyond the tolerance, because skew inside it is expected, but the reconciliation applies either way.

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
//    yet that is exactly the case where tenant rows exceed the model's cost. The warning
//    is for operators; reconciliation does not depend on it: rows are scaled pro rata so
//    they never exceed the model total (Remainder entry, Over-attribution).
// 3. Identity coverage:
//    attributed tokens with a real tenant_id and workload_id / all attributed tokens,
//    where a mapping with no workload label (workloadLabel empty) is judged on tenant_id
//    alone, since a label the producer cannot emit would otherwise score that producer 0%,
//    where "real" means non-empty AND not a missing-identity sentinel. Requirements §4.1
//    lets a producer record missing identity as a sentinel such as "unattributed";
//    a sentinel is a non-empty label value, so checking non-emptiness alone would read
//    100% identity coverage with no real identities. Sentinels stay in the denominator.
//    The sentinel value comes from the producer contract or configuration (§1.9).
//    Warn when < threshold. user_id is optional and excluded.
```

**Engine reference totals.** The reference for token coverage is defined per deployment (`serving_model`, namespace), not as a global sum over engines. Within one engine, SGLang prefill/decode disaggregation reports the full prompt on both workers, so the reference excludes `engine_type="decode"` (and prefill workers for completion tokens); the engine-side queries added for SGLang support (opencost/opencost#4161) already apply this filter, so the coverage check reuses them rather than adding a second query. But `engine_type` only removes an engine's *own* decode workers: a deployment that disaggregates across engines (SGLang prefill with vLLM decode is a real llm-d topology) is counted twice by a sum over engines. Where a deployment mixes engines, `INFERENCE_ATTRIBUTION_REFERENCE_ENGINE` (§1.9) names the engine whose counters are authoritative for that deployment; unset, OpenCost warns and uses the larger of the per-engine totals rather than the sum.

The engine's own metrics stay independent of the attribution series, which is why these comparisons are meaningful. Both sides must use the reset-aware, per-producer deltas of §1.3.2 over the same `[start, end]`, with a tolerance for scrape-boundary skew — which the engine side today does not (prerequisite below).

**Prerequisite: reset-aware engine-side deltas.** The engine reference totals — and the per-million rates themselves — are currently computed by `queryCounterDelta`-style queries that sum replicas before subtracting and substitute the end value on a negative delta. Neither is reset-aware, in either direction (a reset can hide behind a growing counter; see §1.3.2). An engine-side replica reset therefore inflates the per-token rates and can trip the over-attribution warning spuriously. Upgrading the engine-side token queries, including the SGLang queries from opencost/opencost#4161, to the same per-replica `increase()`-based deltas is a prerequisite of this plan. It is a model-level costing fix in its own right — those rates feed the costs of `inference-cost-tracking.md` today — and is tracked as a separate change; until it lands, coverage and over-attribution warnings for windows containing engine-side resets are advisory only.

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

// Attributed series in the last collection with no matching model entry, per producer mapping.
unmatchedKeys = prometheus.NewGaugeVec(
    prometheus.GaugeOpts{
        Name: "llm_dimension_unmatched_keys",
        Help: "Attributed token series that could not be joined to a model entry, per producer mapping.",
    },
    []string{"producer"},
)

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
        Help: "Share of attributed tokens carrying a real tenant_id and workload_id (non-empty, not a missing-identity sentinel), per model and namespace.",
    },
    []string{"model_name", "namespace"},
)
```

**Estimated tokens** (requirements document §7.3). The estimated subtotals on `InferenceDimensionTokens` (§1.1) flow from the query (§1.3.2) through the join to here, and are exported as `llm_dimension_estimated_token_share` — estimated tokens divided by attributed tokens, per model and namespace — so a consumer can tell an estimate from an exact count. The subtotals are subsets of the token totals, so the share needs no extra query.

When `user_id` is empty, the gauge is still emitted with an empty label value. This is valid Prometheus behaviour and preserves `tenant_id` and `workload_id` attribution.

### 1.7 Aggregation and API

**`pkg/inferencecost/aggregate.go`**: add three new dimensions to `supportedAggregateProperties`, three new `case` arms in `aggKey()` and `matchesFilter()`, and three new property-clear guards in `aggregate()`.

**Aggregation keys for dimension entries.** `aggKey()` for a dimension entry must include the dimension fields themselves, not only model and namespace: `computeStep` inserts entries into a map keyed by `aggKey`, so a dimension entry keyed by model and namespace alone overwrites different tenants' entries before aggregation ever runs. The key encoding must also be unambiguous. The existing aggregation joins values with `/`, and dimension values may legitimately contain `/` (tenant ids like `acme-corp/engineering` do), so a `/`-joined key collides `(acme-corp/engineering, rag)` with `(acme-corp, engineering/rag)`. Dimension-aware keys use a separator that cannot appear in the values, or escape (or length-prefix) each component; the same rule applies to the natural key `serving_model:namespace` wherever it is built as a string.

**`pkg/inferencecost/queryservice.go`, `computeStep()`**: take both return values from its own `collector.CollectMetrics()` call and pass them to `collector.BuildDimensionCosts(modelCosts, dimTokens)` immediately after the local `CalculateCosts` call. Dimension-level entries enter the `InferenceCostSet` **only when the request names an attribution dimension** — `tenant_id`, `workload_id` or `user_id` in `aggregate` or `filter` (rule below). Their properties carry all three new fields plus the full model, namespace and cluster properties, so aggregation by `model_name` or `namespace` still sums correctly across tenants, workloads and users.

**Dimension entries in the cost set (settled).** Model-level and dimension-level entries both live in the `InferenceCostSet`, and an aggregation that includes both would count cost twice. The rule:

- Dimension entries are returned **only** when the request names an attribution dimension in `aggregate` or `filter`. Every other query — `aggregate=model_name`, `aggregate=namespace`, no-aggregation totals — is answered from model-level entries alone, and therefore behaves identically whether attribution is enabled or not.
- Because dimension entries, the remainder included, partition the model's cost (§1.5), a query that *does* aggregate dimension entries reaches the same total by the other route.

Two equality tests follow, and both are **release gates** (the implementation does not merge with either failing): `aggregate=model_name` with attribution enabled equals the model-level total, on both cost bases; and the sum over `aggregate=tenant_id` (remainder row included) equals that same total.

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
| `pkg/inferencecost/types.go` | Extend | `UserID/TenantID/WorkloadID` on `InferenceCostProperties`; attribution fields on `Config`, including the list of producer mappings |
| `pkg/inferencecost/apitypes.go` | Extend | `UserID/TenantID/WorkloadID` on `InferenceCostAPIProperties`; update `newInferenceCostResponse` |
| `pkg/inferencecost/collector.go` | Extend | Dimension-token future; `CollectMetrics` returns the dimension result; `BuildDimensionCosts(modelCosts, dimTokens)` with canonicalized model lookup, split costs and remainder; unmatched-key handling; coverage, over-attribution and identity-coverage checks |
| `pkg/inferencecost/aggregate.go` | Extend | Three new dimensions; dimension-aware, unambiguous `aggKey` encoding |
| `pkg/inferencecost/exporter.go` | Extend | `llm_dimension_hourly_cost`, `llm_dimension_unmatched_keys`, `llm_dimension_token_coverage`, `llm_dimension_identity_coverage`; updated `Export` signature |
| `pkg/inferencecost/runner.go` | Extend | Accept the dimension result from `CollectMetrics`; call `BuildDimensionCosts(modelCosts, dimTokens)` after `CalculateCosts()`; pass both slices to `Export()` |
| `pkg/inferencecost/queryservice.go` | Extend | Same `CollectMetrics`/`BuildDimensionCosts` pairing as `runner.go`; `user_id` gating by response exposure, with merge-across-users for unauthorized callers (§1.7 authorization); dimension entries only for attribution-dimension queries (§1.7) |
| `pkg/inferencecost/queryservice_helper.go` | Extend | Updated error messages |
| `docs/inference-cost-tracking.md` | Extend | New env vars; link to the requirements document |

### 1.9 Configuration

New environment variables, read through `pkg/env/costmodel.go` like the existing `INFERENCE_*` variables. The series and label variables below describe a **single** producer mapping — the common case. Clusters running more than one attribution producer configure a **list of mappings** through `INFERENCE_ATTRIBUTION_CONFIG_PATH` instead; when it is set, the per-mapping env vars below are ignored.

| Variable | Default | Purpose |
|---|---|---|
| `INFERENCE_ATTRIBUTION_ENABLED` | `false` | Opt in to querying attribution series and computing dimension costs. When false, model-level costs are unaffected. |
| `INFERENCE_ATTRIBUTION_CONFIG_PATH` | none | Path to a YAML file holding a list of producer mappings (schema below). Set, it replaces the single-mapping env vars. Unset, the env vars apply. |
| `INFERENCE_ATTRIBUTION_INPUT_TOKENS_SERIES` | none (required when enabled) | Full name of the attributed input-token series (counter or histogram `_sum`). |
| `INFERENCE_ATTRIBUTION_OUTPUT_TOKENS_SERIES` | none (required when enabled) | Full name of the attributed output-token series. |
| `INFERENCE_ATTRIBUTION_INPUT_TOKENS_ESTIMATED_SERIES` | none | Sibling series holding estimated input tokens (requirements §7.3). Optional; when set, its deltas are added to attributed totals and exported as the estimated share (§1.6). |
| `INFERENCE_ATTRIBUTION_OUTPUT_TOKENS_ESTIMATED_SERIES` | none | Sibling series holding estimated output tokens. As above. |
| `INFERENCE_ATTRIBUTION_TENANT_LABEL` | `tenant_id` | Label carrying the tenant. |
| `INFERENCE_ATTRIBUTION_WORKLOAD_LABEL` | `workload_id` | Label carrying the workload. Set to empty when the producer has no workload label; identity coverage is then judged on `tenant_id` alone (§1.5). |
| `INFERENCE_ATTRIBUTION_USER_LABEL` | `user_id` | Label carrying the user. Set to empty to disable user-level grouping. |
| `INFERENCE_ATTRIBUTION_MODEL_LABEL` | `serving_model` | Label carrying the **served** model. Must equal the engine's `model_name`. |
| `INFERENCE_ATTRIBUTION_NAMESPACE_LABEL` | `deployment_namespace` | Label carrying the **deployment** namespace, owned by the producer (see §1.3.2). Set it to `namespace` only when the producer runs in the same namespace as the models it meters; the scrape-time `namespace` is otherwise the producer's own. Absent from a series, the namespace is resolved by §1.5. |
| `INFERENCE_ATTRIBUTION_NAMESPACE_MAP` | none | Operator-supplied `serving_model`→namespace mapping for models serving in more than one namespace, as `model=namespace` pairs separated by `;`. Step 2 of the missing-namespace resolution (§1.5). |
| `INFERENCE_ATTRIBUTION_USER_SENTINEL` | none | Value the user label holds when no user is set (for example `"None"` for LiteLLM). Rows carrying it get an empty `user_id`. |
| `INFERENCE_ATTRIBUTION_IDENTITY_SENTINEL` | `unattributed` | The producer's missing-identity sentinel value, excluded from the identity-coverage numerator (§1.5; requirements §7.3). |
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
  # Explicit opt-in to the scrape-time namespace (§1.3.2): valid only because the
  # coordinator is deployed in the same namespace as the models it meters.
  - name: INFERENCE_ATTRIBUTION_NAMESPACE_LABEL
    value: "namespace"
```

Moving the producer later, for example from the coordinator to an AI gateway, is a configuration change.

The config-file form, for a cluster with two producers (a LiteLLM gateway and the llm-d coordinator). Each entry is a full mapping; names and labels are per producer. **The two must meter disjoint traffic** (§1.3.1): here the gateway is authoritative for the models it fronts, and the coordinator is restricted to the models that reach it directly. If the gateway also routes through the coordinator, the coordinator entry would count the same requests again, so its scope must exclude those models:

```yaml
# /etc/opencost/attribution.yaml, via INFERENCE_ATTRIBUTION_CONFIG_PATH
producers:
  - name: litellm-gateway            # labels llm_dimension_unmatched_keys
    # Official LiteLLM's own Prometheus counters, no new producer code. Measured on
    # LiteLLM v1.108.4 (callbacks: ["otel", "prometheus"]); see the notes below.
    inputTokensSeries: litellm_input_tokens_metric_total
    outputTokensSeries: litellm_output_tokens_metric_total
    # No estimated series: LiteLLM marks estimates on its OTel spans only, not on these counters.
    tenantLabel: team_alias
    workloadLabel: ""                 # LiteLLM has no workload label: workload grouping and its identity-coverage check are off for this producer
    userLabel: ""                     # off. To enable: userLabel: user (or end_user) and userSentinel: "None"
    modelLabel: model                 # holds the served model; not requested_model
    namespaceLabel: deployment_namespace   # LiteLLM does not emit it: the namespace is resolved by §1.5
    identitySentinel: "None"          # observed for end_user and user; confirm what team_alias holds without a team
    scope:                            # authoritative for the models fronted by the gateway
      servingModels: [gemma-4-31B]
  - name: llm-d-coordinator
    inputTokensSeries: llm_d_coordinator_request_input_tokens_attributed_total
    outputTokensSeries: llm_d_coordinator_request_output_tokens_attributed_total
    tenantLabel: tenant_id
    workloadLabel: workload_id
    modelLabel: model_name
    namespaceLabel: namespace         # explicit opt-in: coordinator runs in the models' namespace (§1.3.2)
    identitySentinel: none
    scope:                            # only models that reach llm-d without passing the gateway
      servingModels: [llama-3-70b]
```

**The `litellm-gateway` entry** maps the counters official LiteLLM already exports (`litellm_input_tokens_metric_total`, `litellm_output_tokens_metric_total`), with the metric names and labels measured against LiteLLM v1.108.4 in front of vLLM and SGLang, rather than assumed. What that mapping means in practice:

- **Scrape path.** LiteLLM serves its metrics from a mounted app, so the path is `/metrics/` with a trailing slash. Drop `user_email` at the source with LiteLLM's `prometheus_exclude_labels` (requirements §4.2).
- **No workload.** The counters carry team, user, end user, served `model` and `requested_model`, but no workload. Leaving `workloadLabel` empty groups rows by tenant and model only. The identity-coverage check for this producer then uses `tenant_id` alone (§1.5), instead of scoring 0% for a label that cannot exist.
- **No deployment namespace.** The scrape target's `namespace` is the gateway's (§1.3.2), so it is not used. Rows have an empty namespace and go through resolution: exact where the model runs in one namespace, `INFERENCE_ATTRIBUTION_NAMESPACE_MAP` where it runs in several, and the approximate proportional split otherwise. The producer-side fix is an extra label from a deployment mapping, not an OpenCost change.
- **Do not group by `requested_model`.** It is the client's alias, and cardinality grows with it (requirements §7.3). Only the served `model` is the join key. Verify that it equals the engine's `model_name` after any router rewrite; the tested router rewrite case did.
- **Sentinel for absent identity.** `end_user` and `user` carry the literal string `"None"` when unset. Enabling `userLabel` needs `userSentinel: "None"`, so those rows get an empty `user_id` instead of one user called `None`, and are not mistaken for per-user data (§1.7, authorization). The value `team_alias` holds for a request without a team was not reported in the measurement, so `identitySentinel: "None"` is the same assumption and must be confirmed.
- **Estimates and truncated streams.** The counters have no estimated series, so estimated and exact tokens cannot be told apart on this path, and the estimated share (§1.6) reads zero. On a client disconnect mid-stream LiteLLM records the tokens it had streamed as ordinary usage; the engine generated more, and the difference lands in `__unmetered__` rather than with the tenant (requirements §9). This is a producer gap, not an OpenCost one.
- **Cached tokens.** LiteLLM also defines `litellm_input_cached_tokens_metric`. It is not mapped: cache discounts are out of scope (requirements §1), and the series would be the input for a later cache-read rate.
- **SaaS models.** A LiteLLM that also routes to SaaS models emits series for them. Exclude them with `scope.excludeServingModels` (§1.5) or the gateway counts them as unmatched.

`scope` is optional (`servingModels`, `excludeServingModels` and `namespaces`, each a list; empty or absent means all). `excludeServingModels` is how a gateway that also routes to SaaS models keeps them out of the join (§1.5, SaaS and other non-cluster models). Rows outside a mapping's scope are dropped before the join. Omitting it is correct only when the producers already meter different traffic. A single producer needs no scope.

Each mapping's series are queried with the delta strategy of §1.3.2. Entries keep their producer until the per-producer checks (unmatched keys, identity coverage with that producer's sentinel, namespace resolution) have run, and are then summed by identity (§1.3.1): a tenant served through two producers on *different* models or paths is one row; the remainder and token-coverage accounting run over the union. The same tenant on the *same* model through both producers is the double-counting case that `scope` exists to prevent. A producer missing from the file contributes its traffic to the remainder — the failure mode the list exists to avoid is one producer's traffic being silently labeled another's, or nobody's.

### 1.10 Tests

| Test file | What to add |
|---|---|
| `core/pkg/source/decoders_test.go` | `TestDecodeInferenceDimensionResult`, including `user_id` absent |
| `pkg/inferencecost/collector_test.go` | Join formula; zero model cost (no panic); empty attribution result; missing user label; **unmatched key counted and logged, not dropped silently**; **workload label disabled (LiteLLM mapping): rows group by tenant and model, identity coverage uses `tenant_id` alone and does not read 0%; user label with `userSentinel: "None"` yields an empty `user_id`, not a user named `None`; `requested_model` never enters the grouping key**; **SaaS exclusion: a series for a model with no allocation, covered by `excludeServingModels` or outside `servingModels`, is dropped before the join, is not counted unmatched, and does not trip the health check; the same series without a scope is counted unmatched**; **attribution health check: a producer whose keys all carry the scrape-target namespace joins nothing, logs at ERROR with producer and label, and keeps `llm_dimension_unmatched_keys` non-zero; a series without `deployment_namespace` goes through resolution and is never joined to a scrape-target namespace**; coverage, over-attribution and identity-coverage warnings; **identity coverage excludes the missing-identity sentinel from the numerator (a sentinel-only producer scores 0, not 100)**; **remainder entry: tenant rows plus remainder equal the model total under both bases, a model with no attributed series is all remainder, an over-attributed model clamps to zero, and a zero-token window puts the model's full cost in the remainder**; **split costs and per-million rates populated on dimension entries (positive input and output cost, not just totals)**; **canonicalized model lookup: a producer emitting `org/model` joins to the `model` allocation entry**; **missing-namespace resolution: engine-metrics map, then configured mapping, then proportional split across namespaces priced at each namespace's own rate, marked `approximate`, with the split parts deducted from each namespace's remainder (tenant rows plus remainder still equal each namespace total)**; **over-attribution reconciliation: $10 model, 100 engine tokens, 105 attributed tokens gives tenant rows summing to $10 and a zero remainder, with and without the warning firing (within 0.1 tolerance)**; **producer provenance: `none` is a real tenant under producer A and the sentinel under producer B, and identity coverage counts them differently; unmatched keys attributed to the right producer after the merge** |
| `pkg/inferencecost/aggregate_test.go` | Aggregation by each new dimension; filter by `tenant_id:"acme-corp"`; empty `user_id`; **release gates: `aggregate=model_name` with attribution enabled equals the model-level total on both cost bases, and the sum over `aggregate=tenant_id` (remainder included) equals the same total**; **non-attribution queries return no dimension entries**; **`aggKey` collision: `(acme-corp/engineering, rag)` and `(acme-corp, engineering/rag)` remain distinct entries**; **`user_id` aggregate/filter rejected without the enablement flag and without admin auth**; **filter-only request without `user_id` in the query returns no per-user entries for unauthorized callers: entries merged across users, `user_id` stripped** |
| `pkg/inferencecost/exporter_test.go` | `llm_dimension_hourly_cost` emitted correctly; empty `user_id` as empty label; `__unmetered__` row emitted; `llm_dimension_unmatched_keys`; `llm_dimension_token_coverage` and `llm_dimension_identity_coverage` |
| `pkg/inferencecost/env_test.go` or equivalent | Defaults (**namespace label defaults to `deployment_namespace`, not `namespace`**); missing series names disables attribution with an error and leaves model-level collection running; **two mappings from the config file: results merged after per-producer checks (one tenant across two producers is one row), unmatched keys attributed to the right producer, remainder computed over the union; `scope` drops out-of-scope rows before the join; overlapping producers on one `(serving_model, namespace)` without a scope trip the over-attribution check, which names the producers** |
| `modules/prometheus-source/pkg/prom/inference_queries_test.go` | PromQL generated from custom label and series names; user label disabled; one token series absent (graceful degradation); **counter reset mid-window, per producer series (increase semantics, not end-value fallback)**; **multi-replica reset: one replica resets while another keeps counting, deltas stay correct per series**; **exact window: a 1-minute collection window queries `[60s]`, not a widened range, and does not include the preceding minute's traffic**; **new series: a series whose first scrape already reads 100 and then 100 again yields zero increase and a single-sample series is omitted, so their tokens appear in the remainder, not with another tenant; a series created at zero before its first increment is counted in full**; **interior scrape gap: no loss, because the counter is cumulative; a series that stops being scraped mid-window keeps its last value**; **boundary skew: engine and attribution series scraped at different offsets stay within tolerance** |

---

## 2. Key design decisions

| Decision | Chosen approach | Rationale |
|---|---|---|
| **Producer coupling** | Series and label names are configuration — one mapping via env vars, or a list of mappings via config file; no producer-specific discovery | The attribution producer has already moved between components, and may move again, and real clusters run more than one at a time (a gateway in front of a coordinator). OpenCost should not change when producers do. Contract: the requirements document §7.3. |
| **Dimension entries in the cost set** | Returned only when the request names an attribution dimension; equality tests are release gates | Model-level and dimension-level entries in one set double-count when mixed. The remainder row makes dimension entries partition the model cost, so both query routes can reach the same total — and must, verifiably. |
| **Source series type** | Counter (`_total`) or histogram `_sum`, both accepted | Both are monotonically increasing, so the `queryCounterDelta` pattern applies unchanged. |
| **Cost join formula** | `promptTokens x (inputCostPerM / 1M) + completionTokens x (outputCostPerM / 1M)` | Token sums drive cost. Request count is not used, since input and output rates differ. |
| **Calculator call order** | Calculator runs on model entries first; `BuildDimensionCosts` uses the resulting rates | No need to re-run cost-split logic per dimension entry. |
| **`user_id` handling** | Single global label setting; empty when absent | OpenCost does not impose its own cardinality cap. That decision belongs to the producer (requirements document §4.2). |
| **`CollectMetrics` signature** | Extended to return the dimension result; `BuildDimensionCosts` takes it as a parameter | The collector is shared by the background runner and API requests, so a stored result is a data race that pairs one collection's tokens with another's rates. A one-time signature change is cheaper than preserving the pairing incorrectly. |
| **Served-model join key** | `serving_model` joined with deployment namespace, through the canonicalized `combineMetrics` mapping | The served model must equal the engine's `model_name`, so the join is consistent by construction (requirements document F7). The canonicalized mapping keeps producers emitting `org/model` joinable instead of unmatched. |
| **Delta strategy** | Per-producer, reset-aware deltas (`increase()` semantics), then grouped by identity | Summing first and differencing afterwards misattributes replica resets; the end-value fallback undercounts the reset window. |
| **Coverage reference** | Per-deployment engine totals, with `engine_type` exclusions and a configured authoritative engine for cross-engine disaggregation | A global sum over engines double counts deployments that split prefill and decode across engines (SGLang prefill + vLLM decode). |
| **Deployment namespace label** | Separately configurable, default `deployment_namespace` (producer-owned); the scrape-time `namespace` is an explicit opt-in | The scrape-target `namespace` is wrong for gateway-hosted producers, so it cannot be the silent default. A silent mismatch is the main operational risk; the default, the empty-label resolution path and the health check (§1.3.2, §1.5) close it. |
| **`requested_model` excluded** | Not a group-by label | The cost rate follows the served model. Including the requested model inflates cardinality without adding cost signal. |
| **Unmatched series** | Counted, logged and exported, never dropped silently | Label-mapping errors would otherwise look like zero cost. |
| **Rate ownership** | OpenCost is the sole owner of the cost rate for self-hosted models; the attributed series carry tokens only | A metering layer that also prices self-hosted tokens would give two chargeback figures and break the reconciliation of tenant rows plus remainder with the model's cost. The serving stack's records hold identity and tokens only; any money a metering layer handles for its own purposes is outside the contract and is neither read nor reconciled by OpenCost (requirements §1, Rate ownership). |
| **Backward compatibility** | New fields empty by default; feature opt-in | Deployments without attribution are unaffected. |

---

## Appendix A: Multiple deployments

A cluster may hold several independent model deployments, each in its own namespace. OpenCost is deployed once per cluster. The `serving_model:namespace` composite key partitions attributed tokens per deployment automatically, provided the deployment namespace label is correct (§1.3.2). No per-namespace OpenCost configuration is needed.

Producers with different series and label names are supported: each gets its own entry in the mapping list (§1.9). Only the single-mapping env-var form assumes the cluster runs one producer with one naming scheme.

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

### Prerequisite: reset-aware engine-side deltas

The engine-side token queries (`queryCounterDelta` and the SGLang queries from opencost/opencost#4161) sum replicas before subtracting and substitute the end value on a negative delta. A replica reset therefore inflates the per-million rates — a model-level costing defect that exists independently of attribution — and makes the coverage reference unreliable. Upgrading these queries to per-replica, `increase()`-based deltas (§1.3.2) is required before the coverage and over-attribution checks of §1.5 can be treated as authoritative. Tracked as a separate change; see §1.5 for the impact until it lands.

---

## Appendix B: Open questions

- **Idle as its own row (`idle_share`).** The proportional spread of idle under `cost_basis=allocation` is now documented as policy (New outputs, "Idle-cost policy"); what remains open is *exposing* the idle share so a consumer can separate "cost of my requests" from "my share of idle" without reverse-engineering the rate. Today the idle share is inside the per-million rate, so tenants with traffic carry it in proportion to their tokens, and the `__unmetered__` row holds unmetered requests (and, in zero-token windows, the model's retained cost — §1.5). Showing idle separately would need tenant rows priced without idle, with idle in the remainder. The two existing cost queries do not give pure idle: the allocation total is `max(request, usage) x price` plus idle share plus shared-infrastructure share, and the usage total is actual consumption only (`inference-cost-tracking.md`). Their difference mixes idle, request headroom and shared infrastructure. A separate idle row would need a dedicated idle query, and it would change what `cost_basis=allocation` means for tenant rows. Not part of this plan.
