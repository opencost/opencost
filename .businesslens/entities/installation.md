---
singleton: true
references:
- kind: code
  role: implementation
  target: pkg/costmodel/router.go#GetInstallInfo
- kind: code
  role: implementation
  target: pkg/costmodel/router.go#GetHelmValues
---

# Installation

This OpenCost installation in its cluster.

## Information kept

- **Install namespace** — the namespace OpenCost runs in
- **Version** — the OpenCost version
- **Running containers** — the cost analyzer containers running, with their images and start times
- **Cluster size** — the number of nodes and pods in the cluster
- **Helm values** — the Helm values the installation was deployed with, when the chart provides them
