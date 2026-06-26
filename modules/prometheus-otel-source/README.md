# OpenCost Data Sources - Prometheus with OpenTelemetry

The OpenCost Prometheus with OpenTelemetry data source is an implementation which provides OpenCost with the metrics and metadata required to calculate cost allocation. This module extends the standard Prometheus data source to support OpenTelemetry metrics, allowing for a more standardized approach to metrics collection and analysis.

## OpenTelemetry Metrics Support

This module adapts the Prometheus queries to use OpenTelemetry metric names and formats. The table below shows the OTel semantic names, the corresponding Prometheus metric names actually queried (OTel Collector translates dots to underscores), and the original metrics they replace:

| OTel Semantic Name | Prometheus Metric Queried | Replaces |
|--------------------|---------------------------|----------|
| `system.cpu.time` | `system_cpu_time`, `container_cpu_time` | `container_cpu_usage_seconds_total` |
| `system.memory.usage` | `system_memory_usage` | `container_memory_working_set_bytes` |
| `system.filesystem.usage` | `system_filesystem_usage` | `container_fs_usage_bytes` |
| `system.network.io` | `system_network_io` | `container_network_transmit_bytes_total`, `container_network_receive_bytes_total` |
| `k8s.node.cpu.capacity` | `k8s_node_cpu_capacity` | `kube_node_status_capacity_cpu_cores` |
| `k8s.node.cpu.allocatable` | `k8s_node_allocatable_cpu` | `kube_node_status_allocatable_cpu_cores` |
| `k8s.node.memory.capacity` | `k8s_node_memory_capacity` | `kube_node_status_capacity_memory_bytes` |
| `k8s.node.memory.allocatable` | `k8s_node_allocatable_memory` | `kube_node_status_allocatable_memory_bytes` |
| `k8s.pod.container.resource.request` | `k8s_pod_container_resource_request` | `kube_pod_container_resource_requests` |
| `k8s.container.cpu.allocation` | `k8s_container_cpu_allocation` | `container_cpu_allocation` |
| `k8s.container.memory.allocation` | `k8s_container_memory_allocation` | `container_memory_allocation_bytes` |
| `k8s.container.gpu.allocation` | `k8s_container_gpu_allocation` | `container_gpu_allocation` |
| `k8s.persistentvolume.capacity` | `k8s_persistentvolume_capacity` | `kube_persistentvolume_capacity_bytes` |
| `k8s.persistentvolumeclaim.info` | `k8s_persistentvolumeclaim_info` | `kube_persistentvolumeclaim_info` |
| `k8s.persistentvolume.info` | `k8s_persistentvolume_info` | `kubecost_pv_info` |
| `k8s.pod.network.egress` | `k8s_pod_network_egress` | `kubecost_pod_network_egress_bytes_total` |
| `k8s.pod.network.ingress` | `k8s_pod_network_ingress` | `kubecost_pod_network_ingress_bytes_total` |

When validating metric availability in Prometheus, use the Prometheus metric names (middle column).

## Sharded Prometheus Best Practices

**If you are running Prometheus in a sharded (HA) setup:**

- Each Prometheus pod only scrapes a subset of targets. If OpenCost is configured to query a single Prometheus pod, it will only see partial data, and export jobs may fail or return incomplete results.
- To ensure complete and reliable cost data, set `PROMETHEUS_SERVER_ENDPOINT` to a global query endpoint that aggregates all shards, such as [Thanos Query](https://thanos.io/tip/components/query.md/), [Cortex Query Frontend](https://cortexmetrics.io/docs/architecture/), or [Mimir Query Frontend](https://grafana.com/docs/mimir/latest/operations/query-frontend/).
- If you do not use a global endpoint, you may experience intermittent failures or missing data in OpenCost exports.

**Example:**

```
export PROMETHEUS_SERVER_ENDPOINT="http://thanos-query-frontend:9090"
```

For more details, see the [OpenCost documentation](https://www.opencost.io/docs/installation/prometheus) and the documentation for your query aggregator.