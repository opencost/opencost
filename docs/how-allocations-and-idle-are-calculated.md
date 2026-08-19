# How Allocations and Idle are Calculated in Kubecost 3.x

## Overview

Kubecost breaks down every dollar spent running a Kubernetes cluster into the individual workloads that generated the cost. This document explains how **allocation costs** are computed per container and how **idle costs** are derived from the leftover capacity on each node.

---

## Cost Building Blocks

Before allocations can be distributed, Kubecost must know what each node costs. Node cost is computed from pricing data (cloud provider APIs, custom CSV, or default rates) combined with Prometheus metrics:

| Resource | Formula |
|----------|---------|
| CPU | `avg_over_time(cpu_cores) × duration_hrs × $/core-hr` |
| RAM | `avg_over_time(ram_GB) × duration_hrs × $/GB-hr` |
| GPU | `avg_over_time(gpu_cores) × duration_hrs × $/core-hr` |
| Persistent Volume | `avg_over_time(disk_GB) × duration_hrs × $/GB-hr` |
| Network | `bytes_egressed × $/GB` |
| Load Balancer | `forwarding_rules × $/rule + bytes_ingressed × $/byte` |

When the sum of individual resource prices does not match the billed node price, all resource rates are scaled proportionally so that `sum(cpu + ram + gpu) = node_total_cost`.

---

## Allocation Cost per Container

Allocation cost is computed at the **container** level — the smallest unit for which reliable resource usage is available — and is then aggregated to any higher-level dimension (pod, namespace, deployment, label, cluster, etc.).

### Core Formula

```
allocation = max(request, usage)
```

For resources with **allocation costs** (CPU, RAM, GPU), Kubecost charges the greater of what was *requested* (reserved by the Kubernetes scheduler) and what was *actually used*. This means a container that over-requests and under-uses is still charged for its reservation, because those resources were held by the scheduler and unavailable to other workloads.

For resources with **usage costs only** (network, storage I/O), there is no "request" to consider — cost is proportional to actual consumption.

### Per-Resource Allocation Cost

| Resource | Cost Basis |
|----------|-----------|
| CPU | `max(cpuRequestCores, cpuUsageCores) × cpuHourlyRate × hours` |
| RAM | `max(ramRequestBytes, ramUsageBytes) × ramHourlyRate × hours` |
| GPU | `max(gpuRequest, gpuUsage) × gpuHourlyRate × hours` |
| Storage (PV) | `pvcCapacityBytes × pvHourlyRate × hours` |
| Network | proportional share of node network cost |
| Load Balancer | proportional share per service |

### Efficiency

Efficiency is reported separately but does not change cost:

```
cpuEfficiency  = cpuUsage / cpuRequest
ramEfficiency  = ramUsage / ramRequest
```

A container with low efficiency indicates over-provisioned requests; idle costs (see below) capture the unused portion's cost at the cluster level.

---

## Total Cluster Cost Identity

The fundamental accounting identity in Kubecost is:

```
Total Cluster Cost = Workload Costs + Idle Costs + Overhead Costs
```

Where:
- **Workload Costs** — sum of all container allocation costs across all nodes
- **Idle Costs** — node capacity that was neither requested nor used by any workload
- **Overhead Costs** — cluster management fees (e.g. EKS/GKE control-plane hourly fee)

Every dollar in the cloud bill must appear in exactly one of these three buckets.

---

## Idle Cost Calculation

### Definition

Idle is the capacity on a node that no workload has claimed. It is computed **per node** after all container allocations on that node are summed:

```
node_idle_cost = node_total_cost - sum(container_allocation_costs on that node)
```

Broken down by resource:

```
idle_cpu_cost  = node_cpu_cost  - sum(container_cpu_allocation_costs)
idle_ram_cost  = node_ram_cost  - sum(container_ram_allocation_costs)
idle_gpu_cost  = node_gpu_cost  - sum(container_gpu_allocation_costs)
```

> **Key point:** Only resources with *allocation costs* (CPU, RAM, GPU) produce idle. Network and storage are usage-billed and are always 100% efficient — they cannot be idle.

### Negative Idle

When `sum(container_allocation_costs) > node_cost`, idle goes negative. This is almost always caused by one or more containers with **zero requests but significant usage**:

```
# Example:
Node RAM = 8 GB
Pod A: 6 GB request, 1 GB usage  → allocation = 6 GB
Pod B: 0 GB request, 5 GB usage  → allocation = 5 GB
Total allocation = 11 GB > 8 GB node capacity → idle = -3 GB
```

A small negative idle is acceptable; a large negative idle usually indicates a data quality problem in Prometheus metrics or the ETL pipeline.

---

## Idle Pipeline Steps (Kubecost 3.x)

Kubecost 3.x computes idle in a multi-step pipeline inside the FinOps agent and aggregator:

| Step | What Happens |
|------|-------------|
| **Step 4** | Distribute node, network, PV, and LB costs down to each container. Compute container allocation costs including cost adjustments and carbon proportion. |
| **Step 5** | Aggregate all container costs back up to the node level. Handle idle LB cost. |
| **Step 6** | Compute idle resource hours (CPU, RAM, GPU, carbon) per node using `node_cost - sum(container_costs)`. Apply cost adjustment rates from reconciliation. |
| **Step 7** | Aggregate node-level idles to cluster level. Identify completely idle nodes and filter zero-idle entries. |
| **Step 8** | Distribute node-level idle back to containers proportionally via idle coefficients. |
| **Step 9** | Distribute cluster-level (overhead) idle and shared costs (cluster management fees, attached volumes). |
| **Step 10** | Combine node idle + cluster idle + tenancy/shared costs into a single idle line item. |
| **Steps 11–12** | Aggregate to reports by namespace, label, and other dimensions. |

> **Note on reconciliation:** Idle is a **derived value computed after reconciliation**. When cloud billing reconciliation adjusts a node's cost, container workload costs are updated first, and then idle is recomputed. Idle itself does not carry a separate adjustment value.

---

## Idle Display Options

Kubecost gives users four options for how idle costs appear in the Allocations view:

| Option | Behaviour |
|--------|-----------|
| **Hide** | Idle is not shown. Workload costs only. |
| **Separate (Line Item)** | Idle appears as its own row(s) alongside workloads. Default. |
| **Share By Node** | Each node's idle is distributed proportionally across the workloads running on *that node*. |
| **Share By Cluster** | All idle across the cluster is pooled and distributed proportionally across all workloads in *the cluster*. |

### Share By Node vs. Share By Cluster

Consider a cluster with two nodes:

```
Node 1: [w1, w2, w3, w4, idle, idle]   — 4 used, 2 idle
Node 2: [w5, idle, idle, idle, idle, idle]  — 1 used, 5 idle
```

- **Share By Node:** `w1–w4` split the 2 idle units from Node 1; `w5` absorbs all 5 idle units from Node 2.
- **Share By Cluster:** All 5 workloads split the total 7 idle units proportionally.

Idle sharing is applied at the **raw workload level before aggregation**. When you aggregate to namespace or label afterwards, the idle cost is already embedded in each workload's total — the aggregation layer does not re-distribute idle.

---

## Shared Costs

In addition to idle, Kubecost can distribute **shared costs** — system namespace workloads, cluster management fees, and attached volume costs that benefit all tenants. Distribution methods:

1. **Uniformly** — equal share to each tenant
2. **Proportional to consumption** — weighted by a tenant's share of total cluster costs
3. **Custom metric** — e.g. bytes of network egress

---

## API Reference

The `/model/allocation` endpoint (or `/allocation` via `kcctl`) returns `AllocationSetRange` objects for a requested `window`, `aggregate`, and set of `idle`/`shareIdle` options.

Key response fields per allocation:

| Field | Description |
|-------|-------------|
| `cpuCost` | `max(cpuRequest, cpuUsage) × rate × hours` |
| `ramCost` | `max(ramRequest, ramUsage) × rate × hours` |
| `gpuCost` | `max(gpuRequest, gpuUsage) × rate × hours` |
| `pvCost` | PVC capacity × rate × hours |
| `networkCost` | Proportional network cost |
| `lbCost` | Proportional load balancer cost |
| `sharedCost` | Optionally-distributed shared resource cost |
| `idleCost` | Idle cost applied to this workload (when sharing is enabled) |
| `totalCost` | Sum of all above fields |
| `cpuEfficiency` | `cpuUsage / cpuRequest` |
| `ramEfficiency` | `ramUsage / ramRequest` |

---

## Related Pages

- [Allocations v3.x](https://apptio.atlassian.net/wiki/spaces/CSG/pages/1860141075)
- [Idle Allocations](https://apptio.atlassian.net/wiki/spaces/Kubecost/pages/526352780)
- [IBM Docs — Efficiency & Idle](https://www.ibm.com/docs/en/kubecost/self-hosted/3.x?topic=dashboard-efficiency-idle)
- [OpenCost Specification — Idle Costs](https://github.com/opencost/opencost/blob/develop/spec/opencost-specv01.md#idle-costs)
- [Sharing Idle (internal runbook)](https://github.com/kubecost/support/blob/main/architecture-and-technical/sharing-idle.md)
- [Negative Idle Runbook](https://github.com/kubecost/support/blob/main/runbooks/negative-idle.md)
