---
availability:
- place: cost-api::cost-reporting
domain: assets
references:
- kind: code
  role: implementation
  target: pkg/costmodel/handlers.go#ComputeAssetsCarbonHandler
- kind: code
  role: implementation
  target: pkg/carbon/carbonassets.go#RelateCarbonAssets
---

# View asset carbon estimates

Estimate the carbon emissions of the cluster's assets over a window, from the
provider, region and machine type of each node and the provider and region of
each disk. Offered only while carbon estimates are enabled.

Offered only while OpenCost runs in Kubernetes.
