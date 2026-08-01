# Metric aggregator decisions

Newest first.

## 2026-08-01 — QuantileOverTime documents its series-granularity precondition rather than aggregating across series

**Decision:** `QuantileOverTime` keeps pooling every sample it receives into
one flat list. The precondition that makes that correct (the owning
`MetricCollector` must group at or finer than series granularity) is now
stated in the type comment with a worked example, and pinned by
`TestQuantileOverTimeAggregator_PoolsAcrossSeries`. The inference collectors
group by `pod_uid`, which makes one-series-per-group structural.

**Why:** Sean Holcomb observed on
[PR #3915](https://github.com/opencost/opencost/pull/3915) that the aggregator
ignores the label (non-temporal) dimension: with two pods in one group
reporting 2,4,1 and 3,5,1, the flat pool `[1,1,2,3,4,5]` yields a p95 of 4.75,
whereas summing per timestamp first gives `[5,9,2]` and a p95 of 8.6. Both
numbers reproduce exactly, and the mechanism is real:
`MetricCollector.Update` hashes the groupBy label values and hands every
matching sample to a single aggregator, and `MetricAggregator.Update` carries
a value and a timestamp but no series identity, so a coarse grouping is
indistinguishable from one noisy series.

**Alternatives:**

- *Make the aggregator sum same-timestamp samples across series.* Rejected on
  metric semantics, not on effort. Summing is only meaningful for additive
  quantities, and these gauges are not all additive:
  `vllm:kv_cache_usage_perc` is a ratio in [0, 1], so two replicas at 0.4 and
  0.5 must not become 0.9. The shape the Prometheus source implements for the
  same signals is `max by (...) (quantile_over_time(...))` — per-series
  quantile, then an outer combine — which the flat pool coincides with exactly
  when the precondition holds.
- *Teach `MetricAggregator.Update` to carry series identity.* That is a
  breaking change to every aggregator in the package for a problem no
  registered collector currently has. Out of scope for this PR.
- *Change `MaxOverTime` to match.* Not done deliberately: flat max already
  equals the outer-max reading, and altering it would change `RAMUsageMax`,
  `GPUsUsageMax`, `DCGMContainerUsageMax`, `PVUsedMax` and the
  `ResourceQuota*Max` collectors.

**Consequences:**

- Any future collector registered against `QuantileOverTime` with a grouping
  coarser than one series will get flat-pool semantics. The doc comment says
  so, and the regression test fails loudly if the behaviour drifts.
- If a sum-then-quantile aggregator is ever wanted for a genuinely additive
  metric, model it on the accumulate-then-roll-over shape in `increase.go`
  rather than changing this one.
- Related but separate: `util.Hash` concatenates label values with no
  delimiter, so `["ab","c"]` and `["a","bc"]` collide into one aggregator at
  *any* grouping granularity. That is the one path by which Sean's scenario is
  reachable with the collectors as registered today. It affects every
  collector in the module, so it is tracked as its own change rather than
  folded in here.
