# OpenCost Data Sources - Prometheus with OpenTelemetry

The OpenCost Prometheus with OpenTelemetry data source is an implementation which provides OpenCost with the metrics and metadata required to calculate cost allocation. This module extends the standard Prometheus data source to support OpenTelemetry metrics, allowing for a more standardized approach to metrics collection and analysis.

## OpenTelemetry Metrics Support

This module adapts the Prometheus queries to use OpenTelemetry metric names and formats. The key OpenTelemetry metrics used include:

- `system.cpu.time` - CPU usage metrics (replaces `container_cpu_usage_seconds_total`)
- `system.memory.usage` - Memory usage metrics (replaces `container_memory_working_set_bytes`)
- `system.filesystem.usage` - Filesystem usage metrics (replaces `container_fs_usage_bytes`)
- `system.network.io` - Network I/O metrics (replaces `container_network_transmit_bytes_total` and `container_network_receive_bytes_total`)
- `k8s.node.cpu.capacity` - Node CPU capacity (replaces `kube_node_status_capacity_cpu_cores`)
- `k8s.node.cpu.allocatable` - Node allocatable CPU (replaces `kube_node_status_allocatable_cpu_cores`)
- `k8s.node.memory.capacity` - Node memory capacity (replaces `kube_node_status_capacity_memory_bytes`)
- `k8s.node.memory.allocatable` - Node allocatable memory (replaces `kube_node_status_allocatable_memory_bytes`)
- `k8s.pod.container.resource.request` - Container resource requests (replaces `kube_pod_container_resource_requests`)
- `k8s.container.cpu.allocation` - Container CPU allocation (replaces `container_cpu_allocation`)
- `k8s.container.memory.allocation` - Container memory allocation (replaces `container_memory_allocation_bytes`)
- `k8s.container.gpu.allocation` - Container GPU allocation (replaces `container_gpu_allocation`)
- `k8s.persistentvolume.capacity` - PV capacity (replaces `kube_persistentvolume_capacity_bytes`)
- `k8s.persistentvolumeclaim.info` - PVC information (replaces `kube_persistentvolumeclaim_info`)
- `k8s.persistentvolume.info` - PV information (replaces `kubecost_pv_info`)
- `k8s.pod.network.egress` - Pod network egress (replaces `kubecost_pod_network_egress_bytes_total`)
- `k8s.pod.network.ingress` - Pod network ingress (replaces `kubecost_pod_network_ingress_bytes_total`)

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