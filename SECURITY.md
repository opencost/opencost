# OpenCost Security Policy

The OpenCost project greatly appreciates the need for security and timely updates, given our proximity to cloud billing. We are very grateful to the users, security researchers, and developers for reporting security vulnerabilities to us. All reported security vulnerabilities will be carefully assessed, addressed, and responded to.

## Code Security

Application code is version controlled using GitHub. All code changes are tracked with full revision history and are attributable to a specific individual. Code must be reviewed and accepted by a different engineer than the author of the change.

### Dependabot

OpenCost has [Dependabot](https://docs.github.com/en/code-security/supply-chain-security/understanding-your-software-supply-chain/about-supply-chain-security#what-is-dependabot) enabled for assessing dependencies in the project.

## Supported Versions

OpenCost provides security updates for the two most recent minor versions released on GitHub.

For example, if `v1.102.0` is the most recent stable version, we will address security updates for `v1.101.0` and later. Once `v1.103.0` is released, we will no longer provide updates for `v1.101.x` releases.

## Reporting a Vulnerability

The OpenCost project has enabled [Private vulnerability reporting](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing/privately-reporting-a-security-vulnerability) for our repositories which allows for direct reporting of issues to administrators and maintainers in a secure fashion. Please include a thorough description of the issue, the steps you took to create the issue, affected versions, and, if known, mitigations for the issue. The team will help diagnose the severity of the issue and determine how to address the issue. Issues deemed to be non-critical will be filed as GitHub issues. Critical issues will receive immediate attention and be fixed as quickly as possible.

### Kubecost Bug Bounty

Kubecost offers a Bug Bounty program that pays $250 USD for unique, not previously disclosed publicly available CVEs, and accepted security bug reports submitted to vulnerability-report@kubecost.com.

## Disclosure policy

For known public security vulnerabilities, we will disclose the disclosure as soon as possible after receiving the report. Vulnerabilities discovered for the first time will be disclosed in accordance with the following process:

1. The received security vulnerability report shall be handed over to the security team for follow-up coordination and repair work.
2. After the vulnerability is confirmed, we will create a draft Security Advisory on GitHub that lists the details of the vulnerability.
3. Invite related personnel to discuss the fix.
4. Fork the temporary private repository on GitHub, and collaborate to fix the vulnerability.
5. After the fixed code is merged into all supported versions, the vulnerability will be publicly posted in the GitHub Advisory Database.
