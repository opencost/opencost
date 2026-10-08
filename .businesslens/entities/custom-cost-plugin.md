---
domain: custom-costs
relations:
- entity: custom-cost
  verb: reports
  cardinality: one-to-many
references:
- kind: code
  role: implementation
  target: pkg/customcost/pipelineservice.go#getRegisteredPlugins
---

# Custom cost plugin

An external cost source, such as a monitoring or AI vendor, that an operator
installs as a plugin executable with a configuration file. Its name is the
domain of the costs it reports.

## Information kept

- **Domain** — the plugin's name
- **Hourly coverage** — the window of hourly costs imported so far
- **Daily coverage** — the window of daily costs imported so far
