# OpenCost Supply Chain Security Uplift Project

## Overview

This project aims to enhance OpenCost's supply chain security posture by implementing industry-standard artifact attestation, signing, and provenance mechanisms. This will enable users to verify the authenticity and integrity of OpenCost container images and establish unfalsifiable provenance guarantees.

**Status:** Planning
**Target SLSA Level:** Level 3
**Related Issue:** [#3447](https://github.com/opencost/opencost/issues/3447)

## Current State Assessment

### ✅ Existing Security Measures
- Trivy vulnerability scanning
- SonarCloud code quality analysis
- golangci-lint static analysis
- Dependabot automated dependency updates
- OpenSSF Scorecard integration
- Enforced code review and Certificate of Origin
- Multi-stage Docker builds with non-root user
- Team-based maintainer access controls

### ❌ Current Gaps
- No image signing mechanism
- No build provenance (explicitly disabled)
- No artifact attestations
- SBOM generation (in progress - separate PR)
- No consumer verification workflow
- Currently at SLSA Level 1

## Project Goals

1. **Enable verification** - Allow users to cryptographically verify image authenticity
2. **Establish provenance** - Create unfalsifiable record of build process
3. **Improve compliance** - Align with CISA, NIST, and OpenSSF guidance
4. **Reduce user burden** - Eliminate need for downstream re-scanning
5. **Achieve SLSA Level 3** - Meet supply chain security best practices

## Project Phases

### Phase 1: Foundation (Quick Wins)
**Effort:** 1-2 hours
**Priority:** HIGH
**Target:** Week 1

#### Tasks
- [ ] **Enable Build Provenance**
  - Remove `--provenance=false` from `justfile` (lines 74, 87)
  - Update to `--provenance=mode=max`
  - Test provenance generation in CI
  - **Files:** `justfile`
  - **Effort:** 15 minutes

- [ ] **Configure OIDC for Keyless Signing**
  - Add `id-token: write` permissions to release workflow
  - Configure GitHub OIDC trust
  - **Files:** `.github/workflows/release.yml`
  - **Effort:** 15 minutes

- [ ] **Document Current Build Process**
  - Create build process diagram
  - Document security controls
  - **Files:** New `docs/build-process.md`
  - **Effort:** 30-60 minutes

### Phase 2: Image Signing (Core Security)
**Effort:** 2-3 hours
**Priority:** HIGH
**Target:** Week 2

#### Tasks
- [ ] **Implement Cosign Image Signing**
  - Install cosign in release workflow
  - Sign images using keyless (OIDC) mode
  - Sign multi-arch manifests
  - **Files:** `.github/workflows/release.yml`
  - **Effort:** 1-2 hours
  - **Dependencies:** Phase 1 OIDC setup

- [ ] **Verify Signatures in CI**
  - Add signature verification step
  - Test signature validation
  - **Files:** `.github/workflows/release.yml`
  - **Effort:** 30 minutes

- [ ] **Create User Verification Guide**
  - Document how users verify signatures
  - Provide example commands
  - **Files:** `docs/verifying-images.md`
  - **Effort:** 30-45 minutes

### Phase 3: Artifact Attestations
**Effort:** 3-4 hours
**Priority:** MEDIUM
**Target:** Week 3-4

#### Tasks
- [ ] **Implement GitHub Artifact Attestations**
  - Add attestation generation for container images
  - Use `actions/attest-build-provenance`
  - Link build metadata to images
  - **Files:** `.github/workflows/release.yml`
  - **Effort:** 1-2 hours

- [ ] **Attach SBOM to Images**
  - Integrate with SBOM PR work
  - Attach SBOM as attestation using cosign
  - Sign SBOM attestations
  - **Files:** `.github/workflows/release.yml`
  - **Effort:** 1 hour
  - **Dependencies:** SBOM PR completion

- [ ] **Create Attestation Verification Workflow**
  - Provide gh CLI examples
  - Document attestation structure
  - **Files:** `docs/verifying-images.md` (update)
  - **Effort:** 45 minutes

### Phase 4: Enhanced Security & Automation
**Effort:** 4-6 hours
**Priority:** LOW-MEDIUM
**Target:** Week 5-6

#### Tasks
- [ ] **Add Vulnerability Attestations**
  - Attach Trivy scan results as attestations
  - Link scan results to specific image digests
  - **Files:** `.github/workflows/release.yml`, `.github/workflows/trivy.yml`
  - **Effort:** 1-2 hours

- [ ] **Implement Signature Verification in Tests**
  - Add integration tests for signature verification
  - Verify attestations in CI
  - **Files:** New `.github/workflows/verify-attestations.yml`
  - **Effort:** 2-3 hours

- [ ] **Create Security Policy Updates**
  - Update SECURITY.md with verification instructions
  - Document security guarantees
  - **Files:** `SECURITY.md`
  - **Effort:** 1 hour

### Phase 5: Documentation & Promotion
**Effort:** 2-3 hours
**Priority:** MEDIUM
**Target:** Week 7

#### Tasks
- [ ] **Comprehensive Documentation**
  - Complete verification guide
  - Add security architecture docs
  - Create troubleshooting guide
  - **Files:** `docs/security/`, `README.md` updates
  - **Effort:** 1-2 hours

- [ ] **Update Release Notes**
  - Announce attestation support
  - Provide migration guide
  - **Files:** Release notes, blog post
  - **Effort:** 30-60 minutes

- [ ] **OpenSSF Scorecard Improvements**
  - Verify scorecard improvements
  - Address any new recommendations
  - **Effort:** 30 minutes

## Technical Implementation Details

### Image Signing Approach
- **Tool:** Sigstore Cosign
- **Mode:** Keyless signing (GitHub OIDC)
- **Storage:** Transparency log (Rekor)
- **Verification:** Public transparency log

### Attestation Types
1. **Build Provenance** - What was built, when, where, and how
2. **SBOM** - Complete software bill of materials
3. **Vulnerability Scan Results** - Trivy findings
4. **Image Attestation** - GitHub artifact attestation

### Workflow Changes Required

#### `.github/workflows/release.yml`
```yaml
permissions:
  contents: read
  packages: write
  id-token: write  # NEW: For OIDC signing
  attestations: write  # NEW: For artifact attestations
```

#### `justfile`
```makefile
# BEFORE
--provenance=false

# AFTER
--provenance=mode=max
--sbom=true
```

## Success Criteria

### Phase 1
- ✅ Provenance enabled and generating
- ✅ OIDC configured for keyless signing
- ✅ Build process documented

### Phase 2
- ✅ All release images signed with cosign
- ✅ Signatures verifiable via public transparency log
- ✅ User documentation available

### Phase 3
- ✅ Artifact attestations generated for all releases
- ✅ SBOM attached to images
- ✅ Attestations verifiable via gh CLI

### Phase 4
- ✅ Vulnerability attestations included
- ✅ Automated verification in CI
- ✅ Security policy updated

### Phase 5
- ✅ Complete documentation published
- ✅ Release announcement made
- ✅ OpenSSF Scorecard score improved

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Breaking existing workflows | High | Test in feature branch, gradual rollout |
| Signature verification failures | Medium | Comprehensive testing, fallback documentation |
| Performance impact on builds | Low | Signing adds ~30s, acceptable for releases |
| User adoption of verification | Medium | Clear documentation, examples, automation tools |
| Cosign/Sigstore availability | Low | Public infrastructure, high availability |

## Resources & References

### Standards & Frameworks
- [SLSA Framework](https://slsa.dev/)
- [CISA Software Supply Chain Guidance](https://www.cisa.gov/supply-chain)
- [NIST SSDF](https://csrc.nist.gov/Projects/ssdf)
- [OpenSSF Best Practices](https://bestpractices.coreinfrastructure.org/)

### Tools & Documentation
- [Sigstore Cosign](https://docs.sigstore.dev/cosign/overview/)
- [GitHub Artifact Attestations](https://docs.github.com/en/actions/security-guides/using-artifact-attestations-to-establish-provenance-for-builds)
- [Docker Buildx Attestations](https://docs.docker.com/build/attestations/)
- [SLSA Provenance Spec](https://slsa.dev/provenance/v1)

### Related Work
- Issue #3447: Implement artifact attestations
- SBOM PR (in progress)
- Existing Trivy scanning workflow

## Metrics & Tracking

### Key Performance Indicators
- **SLSA Level:** Current Level 1 → Target Level 3
- **OpenSSF Scorecard:** Track improvements in signing, provenance scores
- **Build Time:** Monitor impact (expect +30-60s for signing)
- **User Verification:** Track documentation views, feedback

### Progress Tracking
- GitHub Project board (this document)
- Weekly progress updates in maintainer meetings
- Milestone-based releases

## Team & Ownership

**Project Lead:** TBD
**Reviewers:** OpenCost maintainers
**Timeline:** 6-8 weeks for full implementation
**Effort:** ~15-20 hours total

## Next Steps

1. **Immediate (This Week)**
   - Review and approve project plan
   - Assign project lead
   - Create GitHub Project board
   - Begin Phase 1 implementation

2. **Short Term (Next 2 Weeks)**
   - Complete Phase 1 (Foundation)
   - Begin Phase 2 (Image Signing)
   - Coordinate with SBOM PR

3. **Medium Term (Weeks 3-6)**
   - Complete Phase 2-4
   - Begin documentation
   - Test verification workflows

4. **Long Term (Weeks 7+)**
   - Complete Phase 5
   - Release announcement
   - Monitor adoption and feedback

---

**Document Version:** 1.0
**Last Updated:** 2025-11-16
**Status:** Draft - Awaiting Approval
