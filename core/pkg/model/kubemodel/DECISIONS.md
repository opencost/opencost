# KubeModel entity decisions

Newest first.

## 2026-08-01 — InferenceServer is keyed by pod UID, with no model-level grouping

**Decision:** `InferenceServer` is a flat, per-pod entity keyed by `PodUID` and
stored in `KubeModelSet.InferenceServers` as `map[podUID]*InferenceServer`. It
carries `NamespaceUID` and `ModelName` as fields, and the scheduler gauges
directly. The previous shape (keyed `"model_name:namespace"`, with a
`Replicas map[podName]InferenceServerReplica` and per-entry `Start`/`End`) is
gone, along with the `InferenceServerReplica` type.

**Why:** Review feedback from Sean Holcomb on
[PR #3915](https://github.com/opencost/opencost/pull/3915), and the codebase
agrees with him on every point:

- `Namespace string` (a *name*) appeared exactly once in the non-test kubemodel
  package: on `InferenceServer`. Every other namespace-scoped entity carries
  `NamespaceUID`. Every cross-entity reference in kubemodel is by UID; there
  was no name-based join precedent anywhere.
- Pod names are reused across a pod's lifetime, so a name-keyed replica map
  silently merges a recreated pod with its predecessor. `DCGMDevice.PodUsages`
  (the entity `InferenceServer` cites as its precedent) was already keyed by
  pod UID.
- The per-entry `Start`/`End` were set verbatim from the compute function's
  window arguments, so they only ever restated `KubeModelSet.Window`. Contrast
  `computeDCGMDevices`, which derives them from the result's actual sample
  coverage via `GetStartEnd`, where they carry real information.

**Alternatives:**

- *Keep the model-level grouping, UID-ify the keys* (`ModelName + ":" +
  NamespaceUID`, replicas keyed by pod UID). Rejected: it preserves an
  abstraction that earns nothing at the storage layer. A rollup by served
  model is a view, and a consumer can compute it by grouping on `ModelName`
  more cheaply than the model can maintain a second index.
- *Key the top level by model name alone.* Rejected: the same model name
  served in two namespaces (prod and staging) would collide into one entry.
- *Derive `NamespaceUID` from `kms.Pods[PodUID]` instead of storing it.* It is
  strictly derivable, but storing it keeps `InferenceServer` readable without
  a second lookup and costs one string. `ValidateInferenceServer` therefore
  does not require it — only `PodUID` and `ModelName` are mandatory.

**Consequences:**

- Re-keying cost nothing in compatibility: `InferenceServers` is new at bingen
  field version 3 on this branch and has never shipped, and no code outside the
  register/compute sites reads it.
- `kubemodel_codecs.go` was regenerated with `go generate ./...` in this folder
  (requires `go install github.com/opencost/bingen/cmd/bingen@v0.2.0`).
- Both data sources must supply `pod_uid`. That is free for the collector
  source (the scraper holds the pod object) but requires a scrape-config
  relabel rule for the Prometheus source; see
  `docs/inference-cost-tracking.md`.
- Consumers wanting per-model rollups must group on `ModelName` themselves.
  Nothing consumes `InferenceServers` yet, so no caller was affected.
- `bingen.go` still warns that new fields on serialized structs must be
  appended at the END of a struct. That constraint was not violated here: the
  struct is new at version 3, so its field order was defined fresh.
