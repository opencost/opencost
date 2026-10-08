---
domain: custom-costs
references:
- kind: code
  role: implementation
  target: pkg/customcost/types.go
- kind: code
  role: implementation
  target: protos/customcost/messages.proto
---

# Custom cost

One cost a custom cost plugin reports for an hour or a day, in the FOCUS
billing vocabulary.

## Information kept

- **Domain** — the plugin that reported it
- **Cost source** — the source within the domain
- **Zone** — the zone or region charged
- **Account name** — the account charged
- **Charge category** — the kind of charge, such as usage
- **Description** — the vendor's description of the charge
- **Resource** — the resource name and type charged
- **Provider ID** — the vendor's identifier for the resource
- **Billed cost** — the cost billed
- **List cost** — the cost at list price
- **List unit price** — the list price per unit of usage
- **Usage** — the quantity used and its unit
- **Window** — the hour or day it covers
