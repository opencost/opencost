# prometheus-otel-source

OpenCost data source for clusters that use the **OpenTelemetry Collector** as
their metrics pipeline instead of cAdvisor + node-exporter.

All PromQL queries use OTel-style label names (`k8s_node_name`,
`k8s_namespace_name`, `k8s_pod_name`, …) that the Prometheus translator
produces from OTel resource attributes by replacing dots with underscores.

Enable via environment variable:

```
COLLECTOR_DATA_SOURCE_ENABLED=true
USE_OTEL_LABELS=true
```

---

## Required Collector Components

Three OTel Collector components and KSM must be running. The sections below
describe the exact configuration needed for OpenCost.

### 1. `k8s-node-observer` — kubeletstats + hostmetrics (DaemonSet)

Provides container, pod, node, and volume runtime metrics from the Kubelet API
plus host-level CPU and filesystem metrics.

**Required receiver config:**

```yaml
kubeletstats:
  auth_type: serviceAccount
  endpoint: ${K8S_HOST_IP}:10250
  insecure_skip_verify: true
  metric_groups: [container, node, pod, volume]
  metrics:
    k8s.node.uptime:
      enabled: true     # required: QueryNodeActiveMinutes

hostmetrics:
  root_path: /hostfs
  scrapers:
    cpu:
      metrics:
        system.cpu.utilization:
          enabled: true
    filesystem: {}      # required: QueryLocalStorageBytes / QueryLocalStorageUsed*
```

**Metrics used by OpenCost:**

| Prometheus name | Used for |
|---|---|
| `container_cpu_usage` | CPU usage avg/max |
| `container_memory_working_set` | RAM usage avg/max |
| `k8s_pod_network_io` | Network transfer/receive bytes |
| `k8s_node_uptime` | Node and local-storage active minutes |
| `k8s_node_filesystem_capacity` | Local storage bytes, used avg/max |
| `k8s_node_filesystem_available` | Local storage used avg/max |
| `system_cpu_time` | Node CPU mode breakdown (idle/system/user) |

**Labels on kubeletstats metrics:** `k8s_cluster_name`, `k8s_container_name`,
`k8s_pod_name`, `k8s_namespace_name`, `k8s_node_name`, `k8s_deployment_name`

> The `k8s_attributes` preset must be enabled on the DaemonSet to enrich
> metrics with pod/namespace/node labels.

---

### 2. `k8s-cluster-observer` — k8s_cluster receiver (Deployment)

Provides cluster-level metrics from the Kubernetes API. OpenCost does **not**
currently query the k8s_cluster metrics directly — it uses KSM instead (see
below). However, this collector is required for other platform observability.

**Required receiver config:**

```yaml
k8s_cluster:
  auth_type: serviceAccount
  collection_interval: 10s
  allocatable_types_to_report: [cpu, memory]
```

---

### 3. `k8s-annotated-pod-observer` — KSM scraper (DaemonSet)

Scrapes kube-state-metrics and relabels the classic KSM labels (`namespace`,
`pod`, `node`, …) to OTel-style names (`k8s_namespace_name`, `k8s_pod_name`,
`k8s_node_name`, …) via `metric_relabel_configs`.

This is the **critical component** for OpenCost allocation. Without correctly
relabeled KSM metrics, all allocation queries will return empty results.

**Required relabeling (excerpt from `prometheus/kube-state-metrics` scrape config):**

```yaml
metric_relabel_configs:
  - source_labels: [namespace]
    target_label: k8s_namespace_name
  - source_labels: [pod]
    target_label: k8s_pod_name
  - source_labels: [node]
    target_label: k8s_node_name
  - source_labels: [uid]
    target_label: k8s_pod_uid
  - source_labels: [container]
    target_label: k8s_container_name
  - source_labels: [persistentvolume]
    target_label: k8s_persistentvolume_name
  - source_labels: [persistentvolumeclaim]
    target_label: k8s_persistentvolumeclaim_name
  - source_labels: [storageclass]
    target_label: k8s_storageclass_name
  # ... (drop original labels afterwards)
```

After relabeling, a `transform/ksm_rename_to_otel_attrs` processor in
`otel-collector-service` converts the underscore-format labels to OTel dot
format (`k8s.namespace.name` etc.) for consistent resource attribute handling.

**KSM metrics used by OpenCost:**

| KSM metric | Used for |
|---|---|
| `kube_pod_container_resource_requests` | CPU and RAM requests per container |
| `kube_pod_container_resource_limits` | CPU and RAM limits per container |
| `kube_pod_container_status_running` | Container/pod active minutes |
| `kube_node_status_capacity` | Node CPU and RAM capacity |
| `kube_node_status_allocatable` | Node CPU and RAM allocatable |
| `kube_pod_owner` | Pod → ReplicaSet / DaemonSet / Job ownership |
| `kube_replicaset_owner` | ReplicaSet → Deployment ownership |
| `kube_replicaset_created` | Standalone ReplicaSets (without Deployment) |
| `kube_persistentvolume_capacity_bytes` | PV capacity and active minutes |
| `kube_persistentvolumeclaim_info` | PVC → PV name and StorageClass join |
| `kube_persistentvolumeclaim_resource_requests_storage_bytes` | PVC requested bytes |
| `kube_pod_labels` | Pod label metadata |

**KSM collectors that must be enabled:**

```yaml
collectors:
  - pods
  - nodes
  - persistentvolumes
  - persistentvolumeclaims
  - replicasets
  - deployments
  - daemonsets
  - jobs
```

---

### 4. `otel-collector-service` — central gateway (Deployment)

Receives OTLP from all collectors, enriches with `k8s_attributes`, applies
label cleanup processors, and forwards to Mimir.

**Processors relevant to OpenCost:**

| Processor | Purpose |
|---|---|
| `resource/stage_name` | Injects `k8s.cluster.name` resource attribute |
| `k8s_attributes` | Enriches metrics with pod/namespace/node metadata (excludes KSM and k8s-cluster-observer pods) |
| `transform/ksm_remove_scraper_resource_attrs` | Removes scraper-pod attrs from KSM metrics to prevent wrong labels |
| `transform/metrics_labels_to_attributes` | Promotes resource attributes to datapoint attributes so Prometheus translator emits them as metric labels |
| `transform/remove_noisy_attributes` | Drops `uid`, `container.id` etc. |

> **Important:** The `transform/metrics_labels_to_attributes` processor is what
> makes `k8s_cluster_name`, `k8s_node_name`, etc. appear as Prometheus labels.
> Without it, OTel resource attributes are not promoted to metric labels.

---

## Label Reference

All PromQL queries in this module use these label names:

| Label | Source | Classic equivalent |
|---|---|---|
| `k8s_cluster_name` | `resource/stage_name` processor | `cluster_id` |
| `k8s_node_name` | kubeletstats / KSM relabeling | `node` |
| `k8s_namespace_name` | KSM relabeling / k8s_attributes | `namespace` |
| `k8s_pod_name` | KSM relabeling / k8s_attributes | `pod` |
| `k8s_pod_uid` | KSM relabeling | `uid` |
| `k8s_container_name` | KSM relabeling / k8s_attributes | `container` |
| `k8s_persistentvolume_name` | KSM relabeling | `persistentvolume` |
| `k8s_persistentvolumeclaim_name` | KSM relabeling | `persistentvolumeclaim` |
| `k8s_storageclass_name` | KSM relabeling | `storageclass` |

---

## Unsupported Features

| Feature | Reason |
|---|---|
| GPU cost allocation | No OTel receiver emits GPU utilization metrics |
| ResourceQuota | `kube_resourcequota` not emitted by k8s_cluster receiver |
| PV used bytes | `k8s_volume_name` is the pod volumeMount name, not joinable to PVC names in PromQL |
| `INGEST_POD_UID=true` | Designed for Kubecost's replicated metric setup; not applicable to OTel deployments. A warning is logged and the query falls back to `QueryPods`. |

---

## Diagnostics

The `/otel/diagnostics` endpoint checks whether all required metrics are
present in Prometheus:

```
GET http://localhost:9003/otel/diagnostics
```
