---
appliesTo:
- type: entity
  id: node
  facts:
  - Window
  - Total cost
- type: entity
  id: disk
  facts:
  - Window
  - Total cost
- type: entity
  id: load-balancer
  facts:
  - Window
  - Total cost
references:
- kind: code
  role: implementation
  target: pkg/costmodel/assets.go#ComputeAssets
- kind: code
  role: implementation
  target: pkg/costmodel/assets.go#clampTimeToRange
---

# An asset is charged only for the time it ran within the window

A node, disk or load balancer is costed from when it started to when it ended,
cut to the window asked for, so an asset that ran for part of the window costs
only that part. Nodes running on EKS Fargate are not assets.
