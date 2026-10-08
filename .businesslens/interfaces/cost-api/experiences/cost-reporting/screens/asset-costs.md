---
entities:
- entity: node
  shows:
  - Name
  - Cluster
  - Provider
  - Provider ID
  - Node type
  - Labels
  - Window
  - Capacity
  - Preemptible
  - Discount
  - CPU cost
  - RAM cost
  - GPU cost
  - Total cost
  - Carbon estimate
- entity: disk
  shows:
  - Name
  - Cluster
  - Provider ID
  - Storage class
  - Claim
  - Local
  - Size
  - Window
  - Total cost
  - Carbon estimate
- entity: load-balancer
  shows:
  - Name
  - Cluster
  - IP address
  - Private
  - Window
  - Total cost
  - Carbon estimate
- entity: cluster-management
  shows:
  - Cluster
  - Provisioner
  - Window
  - Total cost
  - Carbon estimate
---

# Asset costs

The cluster's nodes, disks, load balancers and management fees with their costs over a window, and their carbon estimates.
