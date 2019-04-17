Kubecost allows you to export pricing data to Prometheus and then write custom queries for cost insights. Below are instructions for accomplishing this and a set of example queries to get you started.

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

__Calculating the cost of each container (as measured over last 24 hours)__

```
avg_over_time(container_memory_allocation_bytes[24h]) * on(instance) group_left() avg_over_time(node_ram_hourly_cost[24h])  / 1024 / 1024 / 1024
+ 
avg_over_time(container_cpu_allocation[24h]) * on(instance) group_left() avg_over_time(node_cpu_hourly_cost[24h])   
```

__Get memory cost for *default* namespace (measured over the last week)__

```
sum(
  avg_over_time(container_memory_allocation_bytes{namespace="default"}[7d]) 
  * 
  on(instance) group_left() avg_over_time(node_ram_hourly_cost[7d])  / 1024 / 1024 / 1024
) by (namespace)
```

__Monthly cost of currently provisioned nodes__

```
sum(node_total_hourly_cost) * 730
```


## Available Metrics

| Metric       | Description                                                                                            |
| ------------ | ------------------------------------------------------------------------------------------------------ |
| node_cpu_hourly_cost | Hourly cost per vCPU on this node  |
| node_ram_hourly_cost   | Hourly cost per Gb of memory on this node                       |
| node_total_hourly_cost   | Total node cost per hour                       |
| container_cpu_allocation   | Average number of CPUs allocated -- measured by max(Request,Usage)                      |
| container_memory_allocation_bytes   | Average bytes of RAM allocated -- measured by max(Request,Usage)                  |
