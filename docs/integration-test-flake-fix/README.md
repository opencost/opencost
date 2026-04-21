# Proposed Integration-Test Flake Fix

This folder holds a **proposed** patch for
[opencost/opencost-integration-tests](https://github.com/opencost/opencost-integration-tests)
that resolves four flaky tests regularly failing on merge-queue runs of
`opencost/opencost` (for example runs
[24686624556](https://github.com/opencost/opencost/actions/runs/24686624556)
and
[24689201144](https://github.com/opencost/opencost/actions/runs/24689201144)).

The Cursor Cloud Agent that produced this patch only has write access
to `opencost/opencost`, so the actual change needs to be landed in
`opencost/opencost-integration-tests`. The files here are a drop-in
replacement for the current tests, plus a single-commit `.patch` that
can be applied with `git am`.

## Failing tests

All four regularly fail on the same shape of root cause (explained
below):

- `TestPodLabels/Today`
  (`test/integration/api/allocation/pod_labels_test.go`)
- `TestPodAnnotations/Today`, `TestPodAnnotations/Last_Two_Days`
  (`test/integration/api/allocation/pod_annotations_test.go`)
- `TestQueryAllocation/Yesterday`
  (`test/integration/query/count/allocation_running_pods_test.go`)
- `TestQueryAllocationSummary/Yesterday`
  (`test/integration/query/count/allocations_summary_running_pods_test.go`)

A representative failure, taken from run
[24689201144](https://github.com/opencost/opencost/actions/runs/24689201144):

```
--- FAIL: TestPodLabels/Today
    pod_labels_test.go:136: Pod: coredns-74d8fcf7c8-r8m5c
    pod_labels_test.go:143:   - [Fail]: Prometheus Label k8s_app not found in Allocation
    pod_labels_test.go:143:   - [Fail]: Prometheus Label pod_template_hash not found in Allocation
--- FAIL: TestQueryAllocation/Yesterday
    allocation_running_pods_test.go:138: [Fail]: /allocation (135) != Prometheus (136)
```

Diffing the two pod lists in the same run shows the single missing pod
in `/allocation` is the exact same `coredns-74d8fcf7c8-r8m5c` that fails
the label and annotation comparisons.

## Root cause

The tests and OpenCost disagree about "which pods count as running over
the last 24 hours" because they use **different query resolutions**:

- The tests' Prometheus side uses `avg_over_time(kube_pod_container_status_running[24h])`
  (effectively a scrape-rate-resolution average) for the label/annotation
  tests, and the same for the pod-count tests. A pod that was alive for
  even one scrape inside the last 24 hours produces a non-zero value.
- OpenCost's `/allocation` pipeline, in
  `modules/prometheus-source/pkg/prom/metricsquerier.go`
  (`QueryPods` / `QueryPodsUID`), runs
  `avg(kube_pod_container_status_running{} != 0) by (pod, ns, uid, ...)[24h:<N>m]`
  where `<N>` is `DataResolutionMinutes`, defaulting to **5 minutes**
  (see `modules/prometheus-source/pkg/prom/config.go`). That subquery
  produces no sample for a pod that was only alive briefly between two
  5-minute evaluation points, so the pod never enters `podMap` in
  `pkg/costmodel/allocation_helpers.go` and therefore never reaches the
  `/allocation` response.
- Additionally, `kube_pod_labels` and `kube_pod_annotations` are
  published by kube-state-metrics for a small grace period after a pod
  is gone, so a short-lived pod can still appear in those metrics long
  after its last `kube_pod_container_status_running` sample.

The result is: Prometheus (in the test's view) reports 136 pods,
`/allocation` reports 135. For the missing pod, Prometheus also has
labels/annotations and the allocation response has neither → false
negatives on label- and annotation-propagation tests.

A partial fix was already made for `TestPodAnnotations` in
[opencost-integration-tests#68](https://github.com/opencost/opencost-integration-tests/pull/68):
it narrows the Prometheus pod set to pods alive at the exact query
`endTime` using a 1m-resolution subquery. That filter is necessary but
not sufficient — the pod can still be marked alive at `endTime` and
still be absent from `/allocation`, because OpenCost's resolution is
5m, not 1m. So the annotations test continues to fail on the same pod.

## The fix

This patch does two things across the four affected tests:

1. **Apply the PR#68 "alive at endTime" filter to `pod_labels_test.go`
   and to both pod-count tests.** These tests did not have it at all.

2. **Also skip pods that `/allocation` did not return, in both the
   label and annotation tests.** When `AllocLabels` / `AllocAnnotations`
   is nil (because the pod is absent from `/allocation`), comparing
   every Prometheus label/annotation to a nil map will always fail,
   which is noise, not signal. The comparison should only assert
   label/annotation propagation for pods that `/allocation` is actually
   reporting.

The pod-count tests (`allocation_running_pods_test.go`,
`allocations_summary_running_pods_test.go`) already filter by "the
Prometheus pod was non-zero over the 24h window". They now additionally
require the pod to be alive at `endTime` via a 1m-resolution subquery,
which matches the set of pods that `/allocation` (and
`/allocation/summary`) is capable of reporting.

## Files

The proposed replacement test files live under `testdata/` so that the
Go toolchain in this repo ignores them (they import packages from
`opencost-integration-tests`, not from this repo).

- `testdata/pod_labels_test.go` — drop-in replacement for
  `test/integration/api/allocation/pod_labels_test.go`.
- `testdata/pod_annotations_test.go` — drop-in replacement for
  `test/integration/api/allocation/pod_annotations_test.go`.
- `testdata/allocation_running_pods_test.go` — drop-in replacement for
  `test/integration/query/count/allocation_running_pods_test.go`.
- `testdata/allocations_summary_running_pods_test.go` — drop-in
  replacement for
  `test/integration/query/count/allocations_summary_running_pods_test.go`.
- `integration-tests-fix.patch` — the same change as a single
  `git am`-able commit, applied against `main` of
  `opencost-integration-tests`.

## Verification

Starting from a fresh clone of `opencost/opencost-integration-tests`
at `main` (commit `e2dda0a`):

```sh
git checkout -b fix/pod-alive-filter
git am < integration-tests-fix.patch

go vet ./test/integration/api/allocation/... ./test/integration/query/count/...
go test -run '^$' ./test/integration/api/allocation/... ./test/integration/query/count/...
```

Both commands succeed with no output, confirming the modified tests
compile and pass `go vet` cleanly. Runtime validation requires the full
OpenCost test stack (which this repo's CI stands up).

## Why this cannot easily be fixed inside OpenCost itself

Aligning `/allocation` with the test's 1m view of "alive" would require
running the existing `QueryPods` / `QueryPodsUID` subqueries at 1m
resolution instead of `DataResolutionMinutes`. That is ~12× more
rangevector data for every `/allocation` call, a non-trivial
performance regression for every OpenCost user, just to paper over a
test artefact. The semantically correct location for this fix is in
the tests, which is what this patch does.
