# OpenCost Roadmap

This roadmap reflects the current priorities for the OpenCost project. It is reviewed quarterly and discussed in the biweekly [Working Group meetings](https://zoom-lfx.platform.linuxfoundation.org/meetings/opencost?view=list).

## Current Focus Areas

- **Cloud cost integration:** Connecting cloud billing data to the demo environment, cloud cost bug fixes, and multi-account support
- **UI revamp:** Major frontend overhaul via LFX mentorship — new UI released, stabilizing before next core release
- **OpenCost AI:** New sub-project for airgapped private cost models (CI/CD, testing models at scale, finding smallest viable model)
- **First-class LLM cost support:** Design proposal for native LLM cost tracking in OpenCost core
- **Integration test expansion:** Pod restart tests, network cost tests, resolving Prometheus-less (promless) vs Prometheus-backed test discrepancies
- **Plugin ecosystem:** Snowflake, GitHub, and currency conversion plugins proposed; MongoDB reference implementation for currency support
- **Helm chart signing:** Cryptographic signing of Helm charts (research in progress)
- **Data persistence and export:** Mounting persistence for promless mode, potential S3 export for cost data
- **Supply chain security:** Achieving [OpenSSF Best Practices](https://www.bestpractices.dev/projects/6219) Silver and Gold badges, cryptographically signed releases via Sigstore/cosign, SLSA build provenance, and SPDX license compliance across all source files
- **Community growth:** EMEA/APAC meeting cadence, YouTube channel for meeting recordings, DigitalOcean cloud sponsorship for testing

## Recent Milestones

- New OpenCost UI released (v1.0 via LFX mentorship)
- OpenCost AI sub-project introduced (first PR merged)
- MCP server released in v1.118 with right-sizing recommendations
- KubeModel 1.0 shipped (Fall 2025 LFX mentorship)
- SBOM generation integrated across core and UI repos (SPDX + CycloneDX)
- OpenSSF Scorecard integration
- Community Maintainer role introduced
- Gateway API deployed for infrastructure
- Spot node testing enabled in integration test cluster
- Copilot AI review bot enabled across repositories (provided by CNCF)
- OpenCost Specification v0.1 published
- Collector data source shipped (alternative to Prometheus)

## How to Influence the Roadmap

- Join the [OpenCost Working Group](https://zoom-lfx.platform.linuxfoundation.org/meetings/opencost?view=week) (biweekly, alternating between EMEA/APAC at 15:00 UTC and NA at 21:00 UTC)
- Propose changes via [GitHub Issues](https://github.com/opencost/opencost/issues)
- Discuss ideas in the [#opencost](https://cloud-native.slack.com/archives/C03D56FPD4G) channel on [CNCF Slack](https://slack.cncf.io/)
