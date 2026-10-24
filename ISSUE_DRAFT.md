---
name: OpenCost Feature request
about: Suggest an idea for this project
title: 'feat: Add configuration validation and diagnostics API endpoint (GET /config/validate)'
labels: 'feature'
assignees: ''

---

**Is your feature request related to a problem? Please describe.**
Operating OpenCost requires specifying complex environment configurations (Prometheus endpoints, cloud credentials, custom pricing schema files, etc.). If a config file or custom pricing CSV is malformed or invalid, OpenCost often falls back to default settings without providing a diagnostic endpoint for administrators to detect configuration issues.

**Describe the solution you'd like**
Implement a diagnostic endpoint `GET /config/validate` (protected by `ADMIN_TOKEN` security middleware) that aggregates health and validation status across config sources:
1. Prometheus server endpoint ping status and latency.
2. Validation status of custom pricing CSV (parse status, valid row count, syntax error reporting).
3. Health/handshake status of registered CustomCost plugins.

**Describe alternatives you've considered**
Manually checking logs or attempting query requests to infer configuration errors, but a dedicated diagnostic endpoint provides instant visibility into configuration health.

**Additional context**
The endpoint returns a structured JSON payload detailing configuration health and syntax or connection errors.
