# AGENTS.md

OpenCost is a Kubernetes cost monitoring tool written in Go. It provides real-time cost allocation and cloud spend visibility.

## Project Structure

- `cmd/costmodel/` - Main application entry point
- `pkg/` - Core packages (cloud providers, cost model, metrics, MCP server)
- `core/` - Shared core library (separate Go module)
- `modules/` - Additional source modules (prometheus-source, collector-source)
- `ui/` - Frontend UI (separate repo: opencost/opencost-ui)
- `kubernetes/` - Kubernetes manifests
- `protos/` - Protocol buffer definitions

## Dev Environment Setup

**Prerequisites:**
- Go 1.25+
- Docker with `buildx`
- `just` command runner (optional, read `justfile` for manual commands)
- Access to a Kubernetes cluster with Prometheus

**Running locally:**
```bash
# Port-forward Prometheus (if needed)
kubectl port-forward svc/prometheus-server 9080:80

# Run the cost model
cd cmd/costmodel
PROMETHEUS_SERVER_ENDPOINT="http://127.0.0.1:9080" go run main.go
```

**Building:**
```bash
# Build local binary
just build-local

# Or using make
make go/bin
```

## Testing

**Run all unit tests:**
```bash
just test
```

**Run tests for specific modules:**
```bash
just test-core              # Core library tests
just test-opencost          # Main application tests
just test-prometheus-source # Prometheus source module tests
just test-collector-source  # Collector source module tests
```

**Run integration tests** (requires running cluster with Prometheus):
```bash
just test-integration
```

**Code quality:**
```bash
go vet ./...
go fmt ./...
```

## Code Conventions

- Follow standard Go conventions and formatting (`go fmt`)
- Use structured logging (see existing patterns in codebase)
- Environment variables for configuration (see `pkg/env/`)
- Multi-cloud support: AWS, Azure, GCP, Alibaba Cloud
- Custom pricing via CSV for on-prem clusters

## PR Guidelines

- Include `Signed-off-by` in commit messages (DCO required)
- Reference related issues with `Fixes #<issue>` in commit messages
- Run `go fmt` before submitting
- Ensure tests pass: `just test`
- Update `THIRD_PARTY_LICENSES.txt` when adding/updating dependencies

## Key Environment Variables

- `PROMETHEUS_SERVER_ENDPOINT` - Prometheus server URL (required)
- `KUBECONFIG` - Path to kubeconfig file (optional, uses default location)
- `CLOUD_COST_ENABLED` - Enable cloud cost ingestion
- `CLOUD_COST_CONFIG_PATH` - Path to cloud integration config

## Development with Tilt

```bash
# Start local development environment
tilt up
```

See `Tiltfile` and `tilt-values.yaml` for configuration options.
