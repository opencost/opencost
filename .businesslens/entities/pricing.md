---
singleton: true
references:
- kind: code
  role: implementation
  target: pkg/cloud/models/models.go
- kind: code
  role: implementation
  target: pkg/cloud/provider/providerconfig.go
- kind: code
  role: context
  target: configs/default.json
---

# Pricing

The prices OpenCost currently uses: node prices downloaded from the cloud
provider or a pricing CSV, and the custom prices and discounts from the
installation's pricing configuration.

## Information kept

- **Node prices** — the price of each node type, region and spot option known
- **Custom prices** — the configured CPU, spot CPU, RAM, spot RAM, GPU and storage prices
- **Network prices** — the configured prices of zone, region, internet and NAT gateway traffic
- **Discount** — the discount applied to node CPU and RAM
- **Negotiated discount** — a further negotiated discount applied to node CPU and RAM
- **Currency** — the currency prices are in
- **Pricing source summary** — the provider's parsed pricing data
- **Nodes by pricing type** — how many nodes are priced each way, and the total
