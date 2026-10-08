---
references:
- kind: code
  role: implementation
  target: pkg/costmodel/router.go#GetPricingSourceStatus
---

# Pricing source

A source of prices the cloud provider integration uses, such as a provider's
pricing API or a spot data feed.

## Information kept

- **Name** — the source's name
- **Enabled** — whether it is configured
- **Available** — whether OpenCost could read it
- **Error** — why it could not be read
