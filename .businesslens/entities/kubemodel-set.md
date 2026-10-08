---
references:
- kind: code
  role: implementation
  target: core/pkg/model/kubemodel/kubemodel.go
- kind: code
  role: implementation
  target: pkg/kubemodel/pipeline.go
---

# KubeModel set

A snapshot of the cluster's Kubernetes inventory for an hour or a day —
namespaces, workloads, nodes, volumes, pods and devices with their capacities
and usage — that OpenCost exports to storage.

## Information kept

- **Window** — the hour or day it covers
- **Cluster** — the cluster it describes
- **Namespaces** — the namespaces and their resource quotas
- **Workloads** — deployments, stateful sets, daemon sets, jobs, cron jobs, replica sets and services
- **Infrastructure** — nodes, persistent volumes, claims and devices
- **Pods** — pods and their containers
