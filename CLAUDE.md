# CLAUDE.md - OpenCost AI Assistant Guide

This document provides guidance for AI assistants working with the OpenCost codebase.

## Project Overview

OpenCost is an open source Kubernetes cost monitoring tool maintained by the Cloud Native Computing Foundation (CNCF). It provides real-time cost allocation, asset tracking, and cloud cost monitoring for Kubernetes clusters across multiple cloud providers.

**Key Features:**
- Real-time cost allocation by namespace, pod, controller, service, etc.
- Multi-cloud cost monitoring (AWS, Azure, GCP, Alibaba, Oracle, OTC, DigitalOcean, Scaleway)
- Dynamic on-demand pricing via cloud provider APIs
- CSV-based custom pricing for on-prem clusters
- MCP (Model Context Protocol) server for AI agent integration
- Prometheus metrics export

## Repository Structure

```
opencost/
├── cmd/costmodel/          # Main entry point (main.go)
├── core/                   # Core module (shared libraries)
│   └── pkg/
│       ├── clusters/       # Cluster management
│       ├── env/            # Environment variable utilities
│       ├── filter/         # Query filter implementations
│       ├── log/            # Structured logging
│       ├── model/          # Core data models
│       ├── opencost/       # OpenCost domain types (Allocation, Asset, CloudCost)
│       ├── storage/        # Storage abstractions
│       └── util/           # Utility packages
├── modules/
│   ├── collector-source/   # Custom metrics collector (alternative to Prometheus)
│   └── prometheus-source/  # Prometheus data source implementation
├── pkg/
│   ├── cloud/              # Cloud provider implementations
│   │   ├── aws/
│   │   ├── azure/
│   │   ├── gcp/
│   │   ├── alibaba/
│   │   ├── oracle/
│   │   ├── digitalocean/
│   │   ├── scaleway/
│   │   └── otc/            # Open Telekom Cloud
│   ├── cloudcost/          # Cloud cost processing pipeline
│   ├── clustercache/       # Kubernetes cluster caching
│   ├── cmd/costmodel/      # Cost model command implementation
│   ├── config/             # Configuration management
│   ├── costmodel/          # Core cost model logic and API handlers
│   ├── customcost/         # Custom cost plugin support
│   ├── env/                # Environment variable definitions
│   ├── mcp/                # MCP server implementation
│   └── metrics/            # Prometheus metrics
├── configs/                # Default pricing configurations
├── kubernetes/             # Kubernetes manifests (deprecated - use Helm)
├── protos/                 # Protocol buffer definitions
├── spec/                   # OpenCost specification
└── ui/                     # UI components (main UI in opencost/opencost-ui repo)
```

## Development Setup

### Prerequisites

- Go 1.25+ (see go.mod for exact version)
- Docker with `buildx` support
- [just](https://github.com/casey/just) - command runner
- [Tilt](https://tilt.dev/) - for local Kubernetes development
- Kubernetes cluster (local or remote)
- Prometheus instance

### Quick Start Commands

```bash
# Run all unit tests
just test

# Run tests for specific module
just test-core
just test-opencost
just test-prometheus-source
just test-collector-source

# Build local binary
just build-local

# Run locally (requires Prometheus and optionally Kubernetes access)
PROMETHEUS_SERVER_ENDPOINT="http://127.0.0.1:9080" go run ./cmd/costmodel/main.go

# Start development environment with Tilt
tilt up
```

### Running Locally Without Kubernetes

Set `PROMETHEUS_SERVER_ENDPOINT` to your Prometheus URL:

```bash
# Port-forward to Prometheus in your cluster
kubectl port-forward svc/prometheus-server 9080:80

# Run OpenCost
PROMETHEUS_SERVER_ENDPOINT="http://127.0.0.1:9080" go run ./cmd/costmodel/main.go
```

### Running Integration Tests

```bash
INTEGRATION=true just test-integration
```

## Build Commands

```bash
# Build local binary
just build-local

# Build multi-arch binaries
just build-binary <version>

# Build and push Docker image
just build <image-tag> <release-version>

# Validate protobuf definitions
just validate-protobuf
```

## Key Environment Variables

### Core Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `PROMETHEUS_SERVER_ENDPOINT` | (required) | Prometheus server URL |
| `API_PORT` | `9003` | OpenCost API port |
| `CLUSTER_ID` | auto-detected | Cluster identifier |
| `CONFIG_PATH` | `/var/configs` | Configuration directory |

### MCP Server

| Variable | Default | Description |
|----------|---------|-------------|
| `MCP_SERVER_ENABLED` | `true` | Enable MCP server |
| `MCP_HTTP_PORT` | `8081` | MCP server HTTP port |

### Cloud Providers

| Variable | Description |
|----------|-------------|
| `AWS_ACCESS_KEY_ID` | AWS authentication |
| `AWS_SECRET_ACCESS_KEY` | AWS authentication |
| `AZURE_OFFER_ID` | Azure pricing offer ID |
| `AZURE_BILLING_ACCOUNT` | Azure billing account |
| `CLOUD_PROVIDER` | Force cloud provider (aws, azure, gcp, etc.) |
| `USE_CSV_PROVIDER` | Enable CSV-based custom pricing |
| `CSV_PATH` | Path to CSV pricing file |

### Prometheus Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `PROMETHEUS_QUERY_TIMEOUT` | `120s` | Query timeout |
| `PROMETHEUS_QUERY_RESOLUTION_SECONDS` | `300` | Query resolution |
| `MAX_QUERY_CONCURRENCY` | `5` | Concurrent queries |
| `PROM_CLUSTER_ID_LABEL` | `cluster_id` | Cluster ID label name |

### Feature Flags

| Variable | Default | Description |
|----------|---------|-------------|
| `CLOUD_COST_ENABLED` | `false` | Enable cloud cost ingestion |
| `CARBON_ESTIMATES_ENABLED` | `false` | Enable carbon estimation |
| `COLLECTOR_DATA_SOURCE_ENABLED` | `false` | Use collector instead of Prometheus |

## API Endpoints

Main API runs on port 9003 by default:

| Endpoint | Description |
|----------|-------------|
| `GET /allocation` | Cost allocation data |
| `GET /allocation/summary` | Summarized allocation |
| `GET /assets` | Asset cost data |
| `GET /assets/carbon` | Asset carbon estimates |
| `GET /cloudCost` | Cloud cost data |
| `GET /customCost/status` | Custom cost status |
| `GET /metrics` | Prometheus metrics |

## Code Conventions

### Go Style

- Use structured logging via `github.com/opencost/opencost/core/pkg/log`
- Environment variables accessed through `pkg/env` or `core/pkg/env`
- Errors should be wrapped with context

**Before committing, always run:**
```bash
go fmt ./...
go vet ./...
```

### Module Structure

OpenCost uses Go workspace with multiple modules:
- `github.com/opencost/opencost` - Main module
- `github.com/opencost/opencost/core` - Core shared library
- `github.com/opencost/opencost/modules/prometheus-source` - Prometheus integration
- `github.com/opencost/opencost/modules/collector-source` - Metrics collector

When adding dependencies, ensure they're added to the correct module.

### Testing

- Unit tests use standard Go testing (`*_test.go` files)
- Integration tests require `INTEGRATION=true` environment variable
- Use mocks for external dependencies
- Test files should be co-located with implementation

### Logging

```go
import "github.com/opencost/opencost/core/pkg/log"

log.Infof("Processing allocation for window: %s", window)
log.Errorf("Failed to query Prometheus: %v", err)
log.Warnf("Missing pricing data, using defaults")
log.Debugf("Detailed debug information")
```

## Pull Request Guidelines

1. Link related issues using: `Fixes #123`, `Closes #456`
2. Describe user-facing changes and breaking changes
3. Include test coverage for new functionality
4. Run `just test` before submitting
5. Use signed commits (`Signed-off-by` header required)

## Architecture Notes

### Data Flow

1. **Prometheus** collects Kubernetes metrics (CPU, memory, etc.)
2. **OpenCost** queries Prometheus for resource usage data
3. **Cloud Provider** APIs provide pricing information
4. **Cost Model** combines usage × pricing to compute costs
5. **API/MCP** exposes cost data to users and AI agents

### Key Types

- `Allocation` - Cost allocation for a workload over a time window
- `Asset` - Infrastructure asset (node, disk, load balancer)
- `CloudCost` - Cloud service costs from billing APIs
- `Window` - Time range for queries

### Cloud Provider Detection

OpenCost auto-detects the cloud provider from:
1. `CLOUD_PROVIDER` environment variable (explicit override)
2. Kubernetes node labels
3. Instance metadata services

## Common Tasks

### Adding a New Cloud Provider

1. Create package under `pkg/cloud/<provider>/`
2. Implement the `models.Provider` interface
3. Add environment variables in `pkg/env/costmodel.go`
4. Register in `pkg/cloud/provider/provider.go`
5. Add default pricing config in `configs/`

### Adding a New API Endpoint

1. Add handler method to `pkg/costmodel/router.go` or appropriate file
2. Register route in `pkg/cmd/costmodel/costmodel.go`
3. Add tests in corresponding `*_test.go` file

### Modifying Protobuf Definitions

1. Edit `.proto` files in `protos/`
2. Run `./generate.sh` to regenerate Go code
3. Run `just validate-protobuf` to verify

## Cost Model Concepts

Core formulas from the OpenCost Specification (`spec/opencost-specv01.md`):

- **Total Cluster Costs** = Cluster Asset Costs + Cluster Overhead Costs
- **Cluster Asset Costs** = Resource Allocation Costs + Resource Usage Costs
- **Workload Costs** = max(request, usage) for CPU/memory resources
- **Idle Costs** = Allocation costs not attributed to any workload

## Useful Links

- [OpenCost Documentation](https://www.opencost.io/docs/)
- [OpenCost Specification](spec/opencost-specv01.md)
- [Helm Chart](https://github.com/opencost/opencost-helm-chart)
- [UI Repository](https://github.com/opencost/opencost-ui)
- [Integration Tests](https://github.com/opencost/opencost-integration-tests)
- [Plugins](https://github.com/opencost/opencost-plugins)
- [CNCF Slack #opencost](https://cloud-native.slack.com/archives/C03D56FPD4G)
