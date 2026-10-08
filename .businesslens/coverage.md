---
scope: The OpenCost cost model server, its agent mode and its MCP server in the main Go module, with the core library and the Prometheus data source they use.
method: Static inspection of source, configuration and documentation; nothing was built or run.
covered:
- description: Command entry points, server wiring and route registration.
  paths:
  - cmd/costmodel/
  - pkg/cmd/
- description: Cost model handlers, allocation and asset computation, cluster info and pricing endpoints.
  paths:
  - pkg/costmodel/
- description: Allocation and asset filter suggestions.
  paths:
  - pkg/allocation/
  - pkg/asset/
  - core/pkg/autocomplete/
- description: 'Cost domain types: allocations, assets, cloud costs, windows and their aggregation.'
  paths:
  - core/pkg/opencost/
- description: Cloud cost import pipeline and queries.
  paths:
  - pkg/cloudcost/
- description: Cloud integration configuration controller and file watcher.
  paths:
  - pkg/cloud/config/
- description: Custom cost plugin pipeline and queries.
  paths:
  - pkg/customcost/
- description: AI inference cost collection, calculation, queries and metrics.
  paths:
  - pkg/inferencecost/
- description: MCP server tools.
  paths:
  - pkg/mcp/
- description: Carbon estimates and their lookup data.
  paths:
  - pkg/carbon/
- description: KubeModel export pipeline, pruning and computation.
  paths:
  - pkg/kubemodel/
  - core/pkg/model/kubemodel/
  - core/pkg/compute/kubemodel/
- description: Prometheus metric emission and metrics configuration.
  paths:
  - pkg/metrics/
- description: Logging and health endpoints and log exclusion.
  paths:
  - core/pkg/util/apiutil/
  - core/pkg/log/
- description: Environment-variable settings of the cost model.
  paths:
  - pkg/env/
- description: Default pricing configurations.
  paths:
  - configs/
exclusions:
- description: Repository automation, editor settings and development environment.
  paths:
  - .github/
  - .idea/
  - Tiltfile
  - Tiltfile.opencost
  - tilt-values.yaml
  - justfile
  - Makefile
  - tools/
  - generate.sh
- description: Container image build files.
  paths:
  - Dockerfile.cross
  - Dockerfile.debug
- description: Deprecated Kubernetes scrape configuration and UI thumbnail.
  paths:
  - kubernetes/
  - ui/
- description: Exchange-rate client library that nothing in the product imports.
  paths:
  - pkg/currency/
unmapped:
- description: Daily CSV export of container allocations to a local file or object storage.
  paths:
  - pkg/costmodel/csv_export.go
  - pkg/filemanager/
- description: Legacy cost data model endpoint in the cost model router.
  paths:
  - pkg/costmodel/router.go
- description: Cloud provider pricing download, node price lookup and spot pricing per provider.
  paths:
  - pkg/cloud/aws/
  - pkg/cloud/azure/
  - pkg/cloud/gcp/
  - pkg/cloud/alibaba/
  - pkg/cloud/oracle/
  - pkg/cloud/otc/
  - pkg/cloud/ovh/
  - pkg/cloud/scaleway/
  - pkg/cloud/stackit/
  - pkg/cloud/digitalocean/
  - pkg/cloud/ibm/
  - pkg/cloud/provider/
- description: Collector data source that replaces Prometheus by scraping nodes directly.
  paths:
  - modules/collector-source/
- description: Prometheus data source queries behind allocations and assets.
  paths:
  - modules/prometheus-source/
- description: Public and basic pricing modules and the pricing fetch tool.
  paths:
  - modules/pricing/
- description: Cluster cache export and the Kubernetes object cache.
  paths:
  - pkg/clustercache/
  - core/pkg/clustercache/
- description: 'Shared core libraries: storage, exporters, filters, pricing, diagnostics and heartbeat.'
  paths:
  - core/pkg/storage/
  - core/pkg/exporter/
  - core/pkg/filter/
  - core/pkg/pricing/
  - core/pkg/diagnostics/
  - core/pkg/heartbeat/
  - core/pkg/nodestats/
  - core/pkg/source/
- description: External node labels read from ConfigMaps.
  paths:
  - core/pkg/external/
- description: Protocol buffer definitions for the custom cost plugin and KubeModel contracts.
  paths:
  - protos/
limitations:
- description: Cloud cost repair when no window is given.
  paths:
  - pkg/cloudcost/pipelineservice.go
---

# Coverage
