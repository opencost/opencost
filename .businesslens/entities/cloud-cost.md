---
domain: cloud-costs
references:
- kind: code
  role: implementation
  target: core/pkg/opencost/cloudcost.go
- kind: code
  role: implementation
  target: core/pkg/opencost/cloudcostprops.go
---

# Cloud cost

One billed item from a cloud provider for one day, with the five ways the
provider prices it.

## Information kept

- **Provider** — the cloud provider billing it
- **Provider ID** — the provider's identifier for the billed resource
- **Account** — the account ID and name
- **Invoice entity** — the invoice entity ID and name
- **Region** — the region and availability zone
- **Service** — the provider service billed
- **Category** — cluster management, disk, load balancer, network, virtual machine or other
- **Labels** — the provider labels or tags on the item
- **Day** — the day the cost was incurred
- **List cost** — the cost at public list prices
- **Net cost** — the cost after discounts
- **Amortized net cost** — the net cost with up-front commitments spread over time
- **Invoiced cost** — the cost as invoiced
- **Amortized cost** — the cost with up-front commitments spread over time
- **Kubernetes percent** — the share of each cost that went to Kubernetes
