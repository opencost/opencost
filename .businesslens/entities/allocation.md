---
references:
- kind: code
  role: implementation
  target: core/pkg/opencost/allocation.go
- kind: code
  role: implementation
  target: core/pkg/opencost/allocationprops.go
- kind: spec
  role: intent
  target: spec/opencost-specv01.md
  title: OpenCost Specification
---

# Allocation

The cost of a workload over a window. OpenCost computes it per container from
the cluster's usage metrics and the prices of the node, volumes and load
balancers it ran on, then sums containers into whatever grouping the reader
asks for — namespace, controller, pod, label and so on. Idle cost appears as
its own allocation named `__idle__`, and costs with no value for the grouping
appear under `__unallocated__`.

## Information kept

- **Name** — the grouping key, such as a namespace name, `__idle__` or `__unallocated__`
- **Cluster** — the cluster the workload ran in
- **Node** — the node the workload ran on
- **Namespace** — the namespace of the workload
- **Controller kind** — the kind of controller that owns the pod, such as deployment or job
- **Controller** — the name of that controller
- **Pod** — the pod name
- **Container** — the container name
- **Services** — the services selecting the pod
- **Labels** — the pod's and its namespace's labels
- **Annotations** — the pod's and its namespace's annotations
- **Window** — the start and end of the period the costs cover
- **Allocated resources** — the CPU cores, RAM bytes and GPUs charged, the larger of what was requested and what was used
- **Resource requests** — the average CPU, RAM and GPU requested
- **Resource usage** — the average CPU, RAM and GPU used
- **Resource hours** — CPU core hours, RAM byte hours, GPU hours and volume byte hours
- **CPU cost** — the cost of CPU allocated
- **GPU cost** — the cost of GPUs allocated
- **RAM cost** — the cost of memory allocated
- **PV cost** — the cost of persistent volumes claimed
- **Network cost** — the cost of cross-zone, cross-region, internet and NAT gateway traffic
- **Load balancer cost** — the cost of load balancers serving the workload
- **Shared cost** — cost shared into this allocation from others
- **External cost** — cost from outside the cluster attributed to this allocation
- **Idle cost** — the CPU, GPU and RAM cost of capacity no workload was allocated
- **Total cost** — the sum of every cost of the allocation
- **Efficiency** — CPU, RAM, GPU and cost-weighted total efficiency: usage over request
- **Proportional asset resource costs** — each node's and disk's share of cost attributed to this allocation
