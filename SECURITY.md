# OpenCost Security Policy

The OpenCost project greatly appreciates the need for security and timely updates, given our proximity to cloud billing. We are very grateful to the users, security researchers, and developers for reporting security vulnerabilities to us. All reported security vulnerabilities will be carefully assessed, addressed, and responded to.

## Code Security

Application code is version controlled using GitHub. All code changes are tracked with full revision history and are attributable to a specific individual. Code must be reviewed and accepted by a different engineer than the author of the change.

### Dependabot

OpenCost has [Dependabot](https://docs.github.com/en/code-security/supply-chain-security/understanding-your-software-supply-chain/about-supply-chain-security#what-is-dependabot) enabled for assessing dependencies in the project.

## Image Signing and Verification

OpenCost container images published from this repository by the tag-triggered
release workflow and the `develop` branch publishing workflow are signed with
[Sigstore cosign](https://docs.sigstore.dev/cosign/signing/signing_with_containers/)
using **keyless** signatures. Signing is driven by GitHub Actions OIDC — there
are no long-lived signing keys to manage or rotate. Each signature is recorded
in the public [Rekor](https://docs.sigstore.dev/logging/overview/) transparency
log, and every signed image is additionally accompanied by a
[SLSA v1](https://slsa.dev/spec/v1.0/) build provenance attestation produced
with `cosign attest`.

> **Note:** `workflow_dispatch` runs of `build-and-publish-release.yml`
> intentionally skip signing. A manual dispatch runs from a branch ref rather
> than a tag, so the Fulcio certificate identity would not match the
> `refs/tags/vX.Y.Z` pattern that verification tooling and the Kyverno policy
> below pin to — a signature produced under a branch identity would be
> silently rejected by those admission policies anyway. To produce a
> verifiable release, push a `vX.Y.Z` tag and let the tag event trigger the
> workflow.

### What is signed

| Artifact | Registry | Signed by workflow |
|----------|----------|--------------------|
| Release images (`:latest`, `:X.Y.Z`, `:<shorthash>`) | `ghcr.io/opencost/opencost` | `.github/workflows/build-and-publish-release.yml` |
| Develop images (`:develop-latest`, `:develop-<shorthash>`) | `ghcr.io/opencost/opencost` | `.github/workflows/build-and-publish-develop.yml` |
| Helm chart OCI artifacts | `ghcr.io/opencost/opencost-helm-chart` | `opencost/opencost-helm-chart` — `.github/workflows/publish.yml` |

Signatures are attached to the image **by digest**, so any tag that resolves
to a signed manifest is verifiable regardless of tag mutation.

### Expected signing identity

| Field | Value |
|-------|-------|
| `--certificate-oidc-issuer` | `https://token.actions.githubusercontent.com` |
| `--certificate-identity` (release tag `vX.Y.Z`) | `https://github.com/opencost/opencost/.github/workflows/build-and-publish-release.yml@refs/tags/vX.Y.Z` |
| `--certificate-identity-regexp` (any release) | `^https://github\.com/opencost/opencost/\.github/workflows/build-and-publish-release\.yml@refs/tags/v[0-9]+\.[0-9]+\.[0-9]+$` |
| `--certificate-identity-regexp` (develop) | `^https://github\.com/opencost/opencost/\.github/workflows/build-and-publish-develop\.yml@refs/heads/develop$` |

### Verifying an image signature

Install cosign (`go install github.com/sigstore/cosign/v2/cmd/cosign@latest`
or see the [install guide](https://docs.sigstore.dev/cosign/system_config/installation/)),
then verify a specific release:

```bash
VERSION=1.115.0 # replace with the release you are verifying

cosign verify \
  --certificate-identity "https://github.com/opencost/opencost/.github/workflows/build-and-publish-release.yml@refs/tags/v${VERSION}" \
  --certificate-oidc-issuer "https://token.actions.githubusercontent.com" \
  "ghcr.io/opencost/opencost:${VERSION}"
```

A successful verification prints the signed payload and confirms the Rekor
inclusion proof.

### Verifying SLSA provenance

Each image also has a SLSA v1 provenance attestation. Inspect it with:

```bash
VERSION=1.115.0 # replace with the release you are verifying

cosign verify-attestation \
  --type slsaprovenance1 \
  --certificate-identity-regexp "^https://github\.com/opencost/opencost/\.github/workflows/build-and-publish-release\.yml@refs/tags/v[0-9]+\.[0-9]+\.[0-9]+$" \
  --certificate-oidc-issuer "https://token.actions.githubusercontent.com" \
  "ghcr.io/opencost/opencost:${VERSION}"
```

### Verifying the Helm chart signature

Helm chart OCI artifacts published by
[`opencost/opencost-helm-chart`](https://github.com/opencost/opencost-helm-chart)
are signed by the same keyless pattern. Verify a pulled chart with:

```bash
CHART_VERSION=1.45.0 # replace with the chart version you are verifying

cosign verify \
  --certificate-identity-regexp "^https://github\.com/opencost/opencost-helm-chart/\.github/workflows/publish\.yml@refs/tags/v[0-9]+\.[0-9]+\.[0-9]+$" \
  --certificate-oidc-issuer "https://token.actions.githubusercontent.com" \
  "ghcr.io/opencost/opencost-helm-chart/opencost:${CHART_VERSION}"
```

### Admission-time enforcement

The following [Kyverno](https://kyverno.io/) `ClusterPolicy` blocks any pod
whose image is pulled from `ghcr.io/opencost/opencost` unless it carries a
valid keyless signature from the release workflow. An equivalent pattern
works with [Sigstore policy-controller](https://docs.sigstore.dev/policy-controller/overview/)
and [Connaisseur](https://sse-secure-systems.github.io/connaisseur/).

```yaml
apiVersion: kyverno.io/v2beta1
kind: ClusterPolicy
metadata:
  name: verify-opencost-image-signatures
spec:
  validationFailureAction: Enforce
  background: false
  webhookTimeoutSeconds: 30
  rules:
    - name: verify-opencost-signed-by-release-workflow
      match:
        any:
          - resources:
              kinds:
                - Pod
      verifyImages:
        - imageReferences:
            - "ghcr.io/opencost/opencost:*"
          attestors:
            - entries:
                - keyless:
                    subjectRegExp: "^https://github\\.com/opencost/opencost/\\.github/workflows/build-and-publish-release\\.yml@refs/tags/v[0-9]+\\.[0-9]+\\.[0-9]+$"
                    issuer: "https://token.actions.githubusercontent.com"
                    rekor:
                      url: "https://rekor.sigstore.dev"
```

If your cluster pulls development builds, extend `imageReferences` with
`ghcr.io/opencost/opencost:develop-*` and add a second attestor entry whose
`subjectRegExp` targets the `build-and-publish-develop.yml` workflow.

## Supported Versions

OpenCost provides security updates for the two most recent minor versions released on GitHub.

For example, if `v1.102.0` is the most recent stable version, we will address security updates for `v1.101.0` and later. Once `v1.103.0` is released, we will no longer provide updates for `v1.101.x` releases.

## Reporting a Vulnerability

The OpenCost project has enabled [Private vulnerability reporting](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing/privately-reporting-a-security-vulnerability) for our repositories which allows for direct reporting of issues to administrators and maintainers in a secure fashion. Please include a thorough description of the issue, the steps you took to create the issue, affected versions, and, if known, mitigations for the issue. The team will help diagnose the severity of the issue and determine how to address the issue. Issues deemed to be non-critical will be filed as GitHub issues. Critical issues will receive immediate attention and be fixed as quickly as possible.

## Disclosure policy

For known public security vulnerabilities, we will disclose the disclosure as soon as possible after receiving the report. Vulnerabilities discovered for the first time will be disclosed in accordance with the following process:

1. The received security vulnerability report shall be handed over to the security team for follow-up coordination and repair work.
2. After the vulnerability is confirmed, we will create a draft Security Advisory on GitHub that lists the details of the vulnerability.
3. Invite related personnel to discuss the fix.
4. Fork the temporary private repository on GitHub, and collaborate to fix the vulnerability.
5. After the fixed code is merged into all supported versions, the vulnerability will be publicly posted in the GitHub Advisory Database.
