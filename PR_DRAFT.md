## Description
This PR implements a diagnostic endpoint `GET /config/validate` in OpenCost protected by `ADMIN_TOKEN` security middleware (`adminAuthMiddleware`).

Key changes:
1. Registered `GET /config/validate` in `pkg/costmodel/router.go` wrapped with `adminAuthMiddleware`.
2. Implemented `ValidateConfig` handler on `*Accesses` struct aggregating:
   - Prometheus server endpoint ping status, latency, and connectivity errors (`validatePrometheusEndpoint`).
   - Custom pricing CSV file parsing validity, valid row count, and syntax errors (`validateCustomPricingCSV`).
   - Health and executable status of registered CustomCost plugins (`validateCustomCostPlugins`).
3. Added comprehensive unit tests in `pkg/costmodel/router_test.go` covering authentication (503/401/403/200), Prometheus endpoint ping, CSV validation, and plugin discovery.

## Related Issues
Closes #3980

## User Impact
Administrators and FinOps teams can query `GET /config/validate` with `ADMIN_TOKEN` bearer authentication to rapidly inspect configuration health across Prometheus, custom pricing CSV, and CustomCost plugins.

## Testing
1. Added unit tests for validation endpoint authentication, Prometheus ping, CSV parsing, and plugin discovery in `pkg/costmodel/router_test.go`.
2. Verified all tests pass using `CGO_ENABLED=0 go test ./pkg/costmodel/...`.
3. Verified code formatting (`go fmt`) and static analysis (`go vet`).
