# Inference Attribution Requirements for Serving Stacks

Author(s): @simanadler

**Status:** Draft

This document defines what an inference serving stack must make available so that OpenCost can attribute inference cost by **tenant**, **workload**, and **user**. It is engine-neutral: [vLLM](https://vllm.ai/), SGLang, TGI, Triton, and [gateway](./inference-attribution-requirements.md/#10-gateway-candidates) neutral. Each requirement names the *fact* that must be available and which layer may supply it.  

The business need (chargeback, showback, capacity planning and quota enforcement for shared self-hosted models) is described in the [Business need](./inference-cost-attribution-plan.md#business-need) section of the Attribution Plan. This document does not repeat it.

Related documents:

- [Inference Cost Attribution Plan](./inference-cost-attribution-plan.md): the business need, and how OpenCost consumes attribution data to produce per-dimension cost.
- [AI Inference Cost Tracking](./inference-cost-tracking.md): the existing model-level cost solution.

---

## 1. Principle

> **The serving stack owns identity and volume. OpenCost owns cost.**

Attributed cost is a join of two things:

```
attributed cost = tokens(tenant, workload, user, model) x cost-per-token(model)
```

- Cost per token comes from OpenCost's allocation of GPU, idle and shared-infrastructure cost. A serving stack cannot know it and is not asked to.
- Token counts per identity come from the serving stack. That is what this document specifies.

A serving stack never computes, stores or reports a monetary value.

**Cached tokens.** Cost per token is one rate per direction. Cached prompt tokens (F12) are priced at the full input rate: cache discounts are out of scope, so a tenant's attributed cost is an upper bound where cache hits are common. F12 is defined anyway, so a separate cache-read rate can be added later without changing the contract (it would also need F12 on the aggregate counters, Section 7.3).

## 2. Scope

**In scope:** the facts, their failure semantics, the transport options, and the identity and privacy rules needed for attribution.

**Out of scope:**

- Cost rates, idle or shared-infrastructure allocation (OpenCost).
- Authentication and authorization mechanisms. This document states where identity must be verified (Section 4.1) and that the metrics path is inside the trust boundary, but not how to implement either.
- Real-time budget enforcement or request admission based on spend.
- Log aggregation or long-term storage infrastructure.

## 3. Roles

A real deployment spreads the required facts across layers. Requirements are therefore written as "this fact must be available to the metering layer", with a "may be supplied by" column, and not as demands on one component.

| Role | Examples | Typically knows |
|---|---|---|
| **Gateway** | AI gateway, Envoy-based ingress | Authenticated caller, requested model, full response including `usage`, backend or pool it routed to |
| **Router / scheduler** | llm-d router, other model routers | Served model after rewrites, per-request routing decisions, tokenization before inference |
| **Engine** | vLLM, SGLang, TGI | Exact token counts, cached-prefix tokens |
| **Metering** | A component that records or aggregates the facts | Joins identity and volume |
| **OpenCost** | | Cost rates; joins them with metered tokens |

A deployment conforms if, taken together, its layers provide the facts in Section 5 with the semantics in Section 6.

## 4. Identity dimensions

| Dimension | Meaning | Cardinality | Required |
|---|---|---|---|
| `tenant_id` | Organizational unit responsible for the request (customer, team, department) | Bounded | MUST |
| `workload_id` | Application, agent or pipeline generating the request | Bounded | MUST |
| `user_id` | Individual end user within the tenant | Potentially unbounded | MAY |

### 4.1 Trust boundary

Identity MUST be derived or verified at a trusted boundary, normally the gateway that authenticates the caller. Identity supplied by a client header and trusted unchanged lets any client attribute its usage to another tenant.

- The component that sets or verifies the identity is the **only** place such headers may be set. Inner components MUST NOT accept them from untrusted sources.
- Missing identity MUST be recorded as empty or "unattributed". It MUST NOT be silently assigned to a default tenant.
- The metrics path is part of the same trust boundary. OpenCost consumes the aggregate series of Section 7.3, so whoever can write or relabel those series controls billing attribution, regardless of how trustworthy the gateway is. The attributed series MUST reach Prometheus only through an authenticated scrape path, or authenticated remote write (for example mTLS between the producer and the Prometheus instance). A conformance case for this is in Section 9.

### 4.2 `user_id` and privacy

`user_id` may be personal data. Implementations MUST allow the operator to choose how it is emitted:

| Channel | Control |
|---|---|
| Aggregate metric label | Off by default, because unbounded values cause Prometheus cardinality explosion |
| Per-request record | Off, keyed hash (for example HMAC-SHA256 with an operator-supplied secret), or plain, as the operator chooses |

A bare or unkeyed hash of a user identifier is easily reversed by enumeration and does not satisfy the "hashed" option. When `user_id` is omitted from aggregate metrics, OpenCost reports `user_id` as empty and keeps `tenant_id` and `workload_id` attribution.

Per-user cost remains sensitive on the OpenCost side as well: the API restricts `aggregate` and `filter` on `user_id` to explicitly enabled, admin-authenticated callers, and the `user_id` label on the exported metric is subject to Prometheus RBAC (attribution plan, Section 1.7).

## 5. Required facts

Tiers: **MUST** is the baseline for attribution to be correct. **SHOULD** is needed for complete or auditable attribution. **MAY** is an enhancement.

| ID | Fact | Tier | May be supplied by | Notes |
|---|---|---|---|---|
| F1 | `tenant_id` | MUST | Gateway | See Section 4.1 |
| F2 | `workload_id` | MUST | Gateway | |
| F3 | `user_id` | MAY | Gateway | See Section 4.2 |
| F4 | Prompt (input) tokens of the request | MUST | Engine via `usage` in the response; gateway or router when parsing it | Must be the count the engine processed, not a client-side estimate |
| F5 | Completion (output) tokens of the request | MUST | Engine via `usage`; gateway or router when parsing it | |
| F6 | Requested model | MUST | Gateway or router | The model name the client asked for |
| F7 | **Served** model | MUST | Router, engine, or gateway (from its routing config) | The model that actually ran. Differs from F6 on rewrites, aliases and canary routing. Must equal the engine's `model_name` metric label (Section 7.3) |
| F8 | Deployment identity (Kubernetes namespace) | SHOULD. MUST only when the same `serving_model` runs in more than one namespace and OpenCost has no configured mapping | Gateway (from its backend-to-namespace mapping) or router | OpenCost joins on `serving_model` + `namespace`. When absent, OpenCost resolves it (Section 8, step 1) |
| F9 | Prompt tokens for requests that fail after prefill began | SHOULD | Router or engine | See Section 6 |
| F10 | Outcome (success, error class) | SHOULD | Gateway | |
| F11 | Request ID | SHOULD | Gateway | Correlates metering records with traces and logs |
| F12 | Cached prompt tokens | MAY | Engine via `usage` details | Allows reporting cache savings per tenant |

Where F4 and F5 cannot come from the engine's `usage` object (for example a streaming response that never sends one), the metering layer MUST mark the record as `usage_captured=false`. It MUST NOT substitute an estimate without marking it.

## 6. Failure and edge-case semantics

The cases below are where implementations differ most, so each is specified explicitly.

| Case | Prompt tokens (F4/F9) | Completion tokens (F5) | Recorded as consumed? |
|---|---|---|---|
| Success | From `usage` | From `usage` | Yes |
| Rejected before any inference work (auth, malformed body, size limit, tokenization failure) | 0 | 0 | No. No GPU was used. It MAY appear in a request log with outcome `error` |
| Failure after prefill began, before any output (prefill error, decode error) | Tokens known to the stack (for example tokenizer output), flagged `usage_captured=false` | 0 | Yes, prompt tokens only. Prefill cost was incurred |
| Gateway timeout after prefill (the gateway gives up; the engine has prefilled and may keep generating) | As above. The gateway itself has no `usage`; prompt tokens MUST come from engine-side facts, not be recorded as zero | 0 | Yes, prompt tokens only. The engine's own counters still record the work, so it must land in some attributed row or in the remainder, never vanish |
| Stream truncated or interrupted mid-response | As above | 0 if the final `usage` was never delivered. A chunk-count estimate MAY be used if flagged as estimated | Yes, as available |
| Client disconnect | As above | As above | Yes, as available |
| Cache-hit short circuit (the stack answers without decode) | From `usage` if present, else 0 | From `usage` if present, else 0 | As reported |

Notes:

- The engine reports completion tokens only in its final `usage` message. After a failed or truncated stream, completion tokens are unknowable. Do not invent them.
- For streaming OpenAI-style APIs, the metering layer may need to request usage in the stream (`stream_options.include_usage`). If it overrides a client's `false`, it MUST document that the client will receive an extra final chunk with an empty `choices` array.
- Each request MUST be counted at most once. Retries within the stack are one request; client retries are separate requests.

## 7. Transport

Facts reach the metering layer by three kinds of channel. A conforming stack uses whichever fit; the document does not mandate one.

### 7.1 Response body (`usage`)

The default source for F4, F5 and F12. Formats differ across APIs: OpenAI-style `usage.prompt_tokens` and `usage.completion_tokens`; Responses and Anthropic-style `input_tokens` and `output_tokens`; SGLang `meta_info`. The metering layer is responsible for parsing the formats it serves.

### 7.2 Response headers (facts known before the first byte)

Suitable for F7, F8 and F9 when the router knows them before streaming starts (served model after routing, prompt tokens after tokenization). Headers are sent before the body, so they survive a later stream failure. Anything known only at the end of generation MUST use the body or a trailer.

Proposed header names (illustrative, to be agreed with serving-stack maintainers):

| Fact | Proposed header | Required |
|---|---|---|
| Served model | `x-inference-served-model` | Yes |
| Deployment namespace | `x-inference-namespace` | Optional (see F8 and Section 8) |
| Prompt tokens known at routing time | `x-inference-prompt-tokens` | Optional |

Intermediate proxies MUST pass these headers through to the metering layer. Gateways MAY strip them before responding to the end client.

### 7.3 Aggregate metrics (what OpenCost consumes)

OpenCost queries per-dimension token counters from Prometheus. The contract OpenCost needs is:

| Metric (illustrative names) | Type | Labels |
|---|---|---|
| Attributed input tokens | Counter, or histogram `_sum` | `tenant_id`, `workload_id`, `serving_model`, `namespace` (optional, see F8), `user_id` (optional) |
| Attributed output tokens | Counter, or histogram `_sum` | same |

- `serving_model` MUST equal the `model_name` label on the engine's own token metrics (for example `vllm:prompt_tokens_total`). This makes the OpenCost join consistent by construction.
- Metric names are producer-specific today. OpenCost SHOULD treat metric names and label mapping as configuration so that moving the producer between components does not need an OpenCost code change.
- `requested_model` SHOULD NOT be a label on these counters. It inflates cardinality and does not change cost, since price follows the model served.
- Estimated token counts MUST NOT be mixed into the same series as exact counts. A producer that estimates tokens for a request (Section 6) MUST emit them separately: either a sibling series (for example `<input_series>_estimated_total`) or a bounded `estimated="true"` label. Without this, OpenCost cannot tell an estimate from an exact count, and the failure semantics of Sections 5 and 6 are only implementable for per-request records. OpenCost counts estimated tokens as attributed for cost, and exposes the estimated share so a consumer can tell "zero tokens" from "unknown".

### 7.4 Per-request records

For billing-grade or per-user attribution where Prometheus cardinality is impractical, the metering layer SHOULD emit a durable per-request record carrying F1 to F11. Logs are subject to sampling, rotation and backpressure and are not an authoritative billing record. A metering service that stores events durably is.

Where the OpenTelemetry GenAI semantic conventions define an attribute for a fact, the record SHOULD use that attribute name rather than a new one. The attributes below are defined in the [semantic-conventions-genai](https://github.com/open-telemetry/semantic-conventions-genai) registry, where they have `Development` stability. They moved there from the main semantic-conventions repository, which marks them as moved as of v1.45.0. Names may therefore still change.

| Fact | Attribute name |
|---|---|
| F4 Prompt (input) tokens | `gen_ai.usage.input_tokens` |
| F5 Completion (output) tokens | `gen_ai.usage.output_tokens` |
| F6 Requested model | `gen_ai.request.model` |
| F7 Served model | `gen_ai.response.model` |
| F12 Cached prompt tokens | `gen_ai.usage.cache_read.input_tokens` |

Per the conventions, `gen_ai.usage.input_tokens` includes cached tokens, and `gen_ai.usage.cache_read.input_tokens` is a subset of it. F12 is therefore not additive to F4.

The GenAI conventions do not define the identity and attribution facts (F1 to F3, F8), so those keep the names in Section 7.3. F9 to F11 are not mapped here. The conventions define `gen_ai.response.id`, but that identifies the completion, not the gateway's request ID (F11).

The deprecated `gen_ai.usage.prompt_tokens` and `gen_ai.usage.completion_tokens` attribute names MUST NOT be used in new records. They are replaced by `gen_ai.usage.input_tokens` and `gen_ai.usage.output_tokens`.

Engine support, as of 2026-11-10 (checked against the `main` branch of each project and the open pull requests):

| Engine | Emits today | Open change |
|---|---|---|
| SGLang | `gen_ai.usage.prompt_tokens`, `gen_ai.usage.completion_tokens`, non-standard `gen_ai.usage.cached_tokens`, `gen_ai.response.model` (its served model name) | [sgl-project/sglang#49140](https://github.com/sgl-project/sglang/pull/49140) adds `gen_ai.usage.input_tokens`, `gen_ai.usage.output_tokens` and `gen_ai.usage.cache_read.input_tokens`, and keeps setting the deprecated names with the same values |
| vLLM | `gen_ai.usage.prompt_tokens` and `gen_ai.usage.completion_tokens`. `gen_ai.response.model` is defined as a constant but no code sets it on the request span | [vllm-project/vllm#40799](https://github.com/vllm-project/vllm/pull/40799) adds the same three current usage names plus `gen_ai.response.finish_reasons`, and leaves the deprecated names in place |

Both pull requests are open and unmerged. Neither adds `gen_ai.request.model`, and neither engine emits it. That is expected for F6, since the requested model is known to the gateway or router rather than the engine. Because both PRs keep the deprecated names, a metering layer reading engine spans before and after they land can read the current names first and fall back to the deprecated ones. It MUST emit only the current names in its own records.

## 8. What OpenCost does with the facts

1. Queries attributed token sums per `(tenant_id, workload_id, user_id, serving_model, namespace)`. Where `namespace` is absent, OpenCost resolves it in this order:
   1. **Engine metrics.** Build a `serving_model` to namespace map from the engine's own token metrics (`vllm:prompt_tokens_total` or `sglang:prompt_tokens_total`, excluding SGLang decode workers as described in step 4), whose series carry `model_name` and the scrape-time `namespace`. If the model appears in exactly one namespace, use it. Model names are matched with the same canonicalization the existing model-level join uses (for example `google/gemma-4-31B` and `gemma-4-31B`).
   2. **Configuration.** If the model appears in several namespaces, use an operator-supplied mapping (`serving_model` or gateway backend or pool to namespace).
   3. **Blended rate.** If neither resolves it, use the token-weighted average rate across the namespaces serving that model, and mark the result as approximate.
2. Joins them to the per-million-token rates it computed for `(serving_model, namespace)`, under both cost bases (`allocation` and `usage`). This requires OpenCost's model-level cost to be keyed by namespace as well as by model. Where the same model runs in several namespaces, the model-level cost must stay separate per namespace, or the join finds no rate for any of them (see opencost/opencost#4162, fixed by #4163).
3. Exposes the results as `llm_dimension_hourly_cost` and through `/inferenceCost/*` with `aggregate` and `filter` on `tenant_id`, `workload_id` and `user_id`. It also emits a remainder row (`tenant_id="__unmetered__"`) per model and namespace for cost that no attributed series claims, so that tenant rows plus the remainder equal the model's cost.
4. Runs **coverage checks**, per `(serving_model, namespace)`, or per `serving_model` where the namespace is unresolved:
   1. **Token (metering) coverage**, input and output separately: attributed input tokens divided by the engine's prompt-token reference, and attributed output tokens divided by the engine's completion-token reference. Input and output coverage can drift independently (a stream that fails before the final `usage` loses completion tokens only), so one ratio must not stand in for the other. A low ratio means requests are bypassing the metering layer; OpenCost logs a warning. A ratio above one plus a tolerance means over-attribution (producer double counting or window skew) and warns separately.
   2. **Identity coverage**: attributed tokens carrying non-empty `tenant_id` and `workload_id`, divided by all attributed tokens. Token coverage can read 100% while every request lacks identity (Section 6 keeps such requests in the counters), so the two ratios are computed and reported separately. `user_id` is optional (Section 4.2) and is not part of this ratio.

   The engine reference for token coverage is defined **per deployment** (`serving_model`, namespace), not as a global sum over engines. In SGLang prefill/decode disaggregation, both workers report every request's full prompt, labelled `engine_type="prefill"` or `"decode"` (`"unified"` otherwise), so the reference MUST exclude decode workers (`engine_type!="decode"`), and for completion tokens exclude prefill workers (`engine_type!="prefill"`). The same exclusion applies when building the namespace map in step 1. But the `engine_type` filter only removes an engine's *own* decode workers: a deployment that disaggregates across engines (SGLang prefill with vLLM decode is a real llm-d topology) is still counted twice by a sum over engines. Where a deployment mixes engines, the operator configures which engine is authoritative for its reference (attribution plan, Section 1.5).

The coverage check is why the engine's own token metrics remain independent of the metering path. They are the reference the attributed totals are compared against.

## 9. Conformance

A deployment can be checked with a fixed set of requests against its stack, comparing metered output with expected values:

| Test | Expected |
|---|---|
| Non-streaming success with identity | Tokens equal engine `usage`; identity labels present; one record |
| Streaming success | Same, with usage captured from the final chunk |
| Request without identity | Recorded as unattributed; not assigned to any tenant |
| Model rewritten by the router | `serving_model` is the rewritten model; requested model recorded separately (F6) |
| Failure after prefill | Prompt tokens recorded, completion 0, `usage_captured=false` |
| **Gateway timeout after prefill** | The gateway gives up while the engine has already done prefill (and may keep generating). Prompt tokens come from engine-side facts (F9, response headers or engine counters), recorded with `usage_captured=false` — never silently zero, and never dropped from the engine's reference total |
| Rejected before inference | No consumption recorded |
| Client disconnect mid-stream | Prompt tokens recorded; completion tokens flagged as unavailable or estimated |
| **Forged identity header** | A client sends an identity header naming another tenant, while authenticating as someone else. Attribution follows the authenticated identity (or none); the forged value appears nowhere in the output. Every engine-counted token is still attributed or visible as remainder |
| **Unauthorized metrics write** | A writer without scrape-path or remote-write credentials cannot alter the attributed series (checked at the deployment level, not per request) |
| Coverage | Attributed input and output tokens each within a stated tolerance of the engine's reference totals; identity coverage reported separately |

## 10. Gateway candidates

Assessments below are preliminary and not verified against current releases. Before selecting a gateway, confirm its licence, that it has had a release within the last six months, and the checklist that follows.

| Gateway | Base | Strengths for attribution | Concerns |
|---|---|---|---|
| **Envoy AI Gateway** | Envoy Gateway, Kubernetes Gateway API (Apache 2.0) | Reads token usage from responses including streaming; routes by model; token-based rate limiting; OpenTelemetry GenAI metrics; same Envoy data plane as llm-d | Relatively young. Unclear whether token metrics accept custom identity labels |
| **LiteLLM proxy** | Python proxy (MIT core) | Virtual keys, teams and users map to tenant, workload and user; per-key spend tracking; Prometheus metrics | Computes cost from a price list, overlapping with OpenCost (Section 11). Needs a database. Some governance features are enterprise-only |
| **kgateway** | Envoy, CNCF | AI routing; supported gateway for the Gateway API Inference Extension | AI metering less mature than routing |
| **Kong Gateway (AI plugins)** | Nginx/OpenResty | Mature; identity plugins; token metrics | Advanced AI features in the enterprise tier |
| **Apache APISIX** (`ai-proxy` plugins) | Nginx/OpenResty | Apache-licensed core; plugin model | Less Kubernetes-native; smaller AI community |
| **Portkey Gateway** | Node/TypeScript | Lightweight; good observability | Governance features in the hosted product |
| **Higress** | Envoy/Istio | AI plugins | Community largely outside the llm-d ecosystem |
| **agentgateway** | Rust | Aimed at agents and MCP | Early stage |

The Gateway API Inference Extension endpoint picker used by llm-d is a router, not a gateway. It complements a gateway by supplying F7 and F8.

**Suggested choices:**

- **Envoy AI Gateway.** It is Kubernetes-native and built on the Gateway API. Authentication through JWT or OIDC security policies can produce trusted identity headers (Section 4.1). It parses `usage` (F4, F5) and knows the backend it routed to (F7, F8).
- **LiteLLM in front of a plain vLLM.** It is a different architecture from an Envoy-based gateway, so evaluating both shows the contract is general. It would be configured to export tokens only, not cost, which forces the rate-ownership question in Section 11.

**Selection checklist.** For each candidate, verify:

1. Token counters can carry custom labels (`tenant_id`, `workload_id`, `serving_model`, deployment `namespace`), with `user_id` optional.
2. Prompt tokens are recorded for requests that fail after prefill (F9).
3. Streaming responses without `usage` are marked `usage_captured=false`.
4. Missing identity is recorded as unattributed, not defaulted.
5. `serving_model` equals the engine's `model_name` after rewrites.

If no gateway passes item 1, a small ext-proc metering service is the fallback. Section 5 allows the facts to come from any layer.

## 11. Open questions

- **Standard names (largely resolved).** The GenAI semantic conventions cover requested model, response model, input and output tokens, and cache-read input tokens (F4 to F7, F12), and Section 7.4 uses those names for per-request records. They are at `Development` stability in the semantic-conventions-genai registry, so the names could still change. Still open: whether the Section 7.2 header names and the Section 7.3 metric names should follow GenAI metric conventions (the registry has `model/gen-ai/token-metrics.yaml`, not yet reviewed), and the names for the identity facts (F1 to F3, F8), which the conventions do not define.
- **Header names.** The names in Section 7.2 are placeholders pending agreement with serving-stack maintainers.
- **Metering ownership.** Whether per-request metering of record lives in the gateway, in a dedicated metering service, or both, and whether that component also computes cost. If it computes cost from list prices it would overlap with OpenCost for self-hosted models, and one source of truth for rates must be chosen.
- **Namespace discovery.** Whether OpenCost-side resolution (Section 8, step 1) is sufficient in practice, or whether the gateway's backend-to-namespace mapping or the router should also return the namespace for models that run in several namespaces.
- **Completion tokens on truncated streams.** Whether an estimate flagged as such is acceptable for billing, or such requests should be reported as unavailable only.
