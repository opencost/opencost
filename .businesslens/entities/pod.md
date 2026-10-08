---
references:
- kind: code
  role: implementation
  target: pkg/costmodel/router.go#GetOrphanedPods
---

# Pod

A Kubernetes pod in the cluster.

## Information kept

- **Name** — the pod name
- **Namespace** — its namespace
- **Labels** — its labels
- **Annotations** — its annotations
- **Owner references** — the controllers that own it; none for an orphaned pod
- **Status** — its Kubernetes status
