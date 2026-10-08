---
kind: primary
routes:
  metrics: Metrics endpoint
steps:
- text: The Metrics scraper scrapes the metrics endpoint
  kind: actor
  actor: metrics-scraper
  entities: []
  contexts:
    metrics:
      place: prometheus-metrics
- text: The Product returns the hourly prices of each Node, Disk, Load balancer and Cluster management fee
  kind: product
  actor: metrics-scraper
  entities:
  - entity: node
    effect: reads
    facts:
    - Name
    - Node type
    - Provider ID
    - Hourly prices
    - Capacity
    - Preemptible
  - entity: disk
    effect: reads
    facts:
    - Name
    - Provider ID
    - Hourly price
  - entity: load-balancer
    effect: reads
    facts:
    - Service
    - IP address
    - Hourly cost
  - entity: cluster-management
    effect: reads
    facts:
    - Provisioner
    - Hourly cost
  contexts:
    metrics:
      place: prometheus-metrics
- text: The Product returns each container's Allocation of CPU, memory and GPUs and the Network prices of the Pricing
  kind: product
  actor: metrics-scraper
  entities:
  - entity: allocation
    effect: reads
    facts:
    - Namespace
    - Pod
    - Container
    - Node
    - Allocated resources
  - entity: pricing
    effect: reads
    facts:
    - Network prices
  contexts:
    metrics:
      place: prometheus-metrics
- text: The Product returns the Cluster's info and each Pod's labels and owners
  kind: product
  actor: metrics-scraper
  entities:
  - entity: cluster
    effect: reads
    facts:
    - ID
    - Name
    - Provider
    - Provisioner
  - entity: pod
    effect: reads
    facts:
    - Name
    - Namespace
    - Labels
    - Owner references
  contexts:
    metrics:
      place: prometheus-metrics
---

# Scrape cost model metrics

## Trigger

Prometheus scrapes OpenCost on its interval.

## Outcome

Prometheus holds the latest prices and allocations, from which OpenCost later computes costs.

## Edge cases

- While inference costs are enabled, each served model's hourly Inference cost, cost per million tokens and cache savings are included.
- Deprecated Kubernetes metrics are left out unless they are explicitly enabled.
