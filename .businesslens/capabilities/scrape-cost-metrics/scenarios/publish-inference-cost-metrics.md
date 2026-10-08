---
kind: alternative
routes:
  metrics: Metrics endpoint
steps:
- text: The collection interval for model serving costs has passed
  kind: condition
  unattended: true
  entities:
  - entity: opencost-settings
    effect: reads
    facts:
    - Inference costs enabled
  contexts:
    metrics:
      place: prometheus-metrics
- text: The Product computes each served model's Inference cost for the interval and publishes it for the next scrape
  kind: product
  entities:
  - entity: inference-cost
    effect: reads
    facts:
    - Model
    - Namespace
    - Hourly cost
    - Cost per million tokens
    - Input cost
    - Output cost
    - Cache savings fraction
    - Allocation method
  contexts:
    metrics:
      place: prometheus-metrics
---

# Publish inference cost metrics

## Trigger

The inference collector's interval passes.

## Outcome

The next scrape receives each served model's latest hourly cost, cost per million tokens and cache savings.

## Edge cases

- A failed collection publishes nothing new; the metrics from the last successful collection remain.
