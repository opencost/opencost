Kubecost allows you to export pricing data to Prometheus and then write custom queries to gain cost insights. Below are instructions for accomplishing this.

## Configuration

After deploying the Kubecost model (see [README](README.md) for more info on installation), configure Prometheus to scrape the `/metrics` endpoint exposed by Kubecost. Below is a sample scrape config:

```
- job_name: kubecost
  honor_labels: true
  scrape_interval: 1m
  scrape_timeout: 10s
  metrics_path: /metrics
  scheme: http
  static_configs:
  - targets:
    - kubecost-cost-analyzer.kubecost:9003
``` 

## Example queries

Below are a set of sample queries that can be run after Prometheus begins ingesting Kubecost data:

__Calculate cost of running each container (as measured over last 24 hours)__

```
avg_over_time(container_memory_allocation_bytes[24h]) * on(instance) group_left() avg_over_time(node_ram_hourly_cost[24h])  / 1024 / 1024 / 1024
+ 
avg_over_time(container_cpu_allocation[24h]) * on(instance) group_left() avg_over_time(node_cpu_hourly_cost[24h])   
```

__Get memory cost for *default* namespace (measured over the last week)__

```
sum(
  avg_over_time(container_memory_allocation_bytes{namespace="default"}[24h]) 
  * 
  on(instance) group_left() avg_over_time(node_ram_hourly_cost[24h])  / 1024 / 1024 / 1024
) by (namespace)
```

__Monthly cost of currently provisioned nodes__

```
sum(node_total_hourly_cost) * 730
```


## Available Metrics

* node_cpu_hourly_cost
* node_ram_hourly_cost
* node_total_hourly_cost
* container_memory_allocation_bytes
* container_cpu_allocation
