# kubemodel testdata

## `kubemodelset-v3.bin`

A `KubeModelSet` encoded by the **version-3** codec, before the sample-window fields were added to
`InferenceEngine`.

**This file must never be regenerated.** It is a frozen historical artifact. The current encoder emits version 4,
so re-running any generator against current code would produce a version-4 payload and silently destroy the only
test we have that a newer reader decodes an older payload. If it is ever lost, recovering it means checking out a
commit from before the version-4 bump and re-encoding there.

Consumed by `TestVersion3PayloadDecodesWithoutSampleWindowFields`.

### Contents, and why they are what they are

- One `Cluster` named `v3-fixture-cluster`.
- **Two** `InferenceEngine` entries, `pod-uid-alpha/0` and `pod-uid-beta/1`, with distinctive non-repeating gauge
  values (0.11, 0.22, 0.33, 1.5, 2.5, ...).

The two-engine detail is load-bearing rather than incidental. The version gate on a new field is only observable
if a mis-gated read has something to corrupt. With a single engine, reading four stray bytes for a field that
should have been skipped lands on trailing zeros: the value stays zero, and an assertion that the field is zero
still passes. That is not a hypothetical — the first version of this fixture had one engine, and mutating the
`SampleCount` gate from `version >= 4` to `version >= 3` did not fail the test.

With two engines, a stray read in the first desynchronises the second, and the distinctive values make the
resulting garbage obvious. The mutation now fails with `Should be zero, but was 8`.

Any future fixture added here for the same purpose should follow the same rule: at least two instances of the
entity whose fields are version-gated, with values chosen so that misalignment produces an implausible number.
