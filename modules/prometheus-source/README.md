# OpenCost Data Sources - Prometheus

The OpenCost Prometheus data source is an implementation which provides OpenCost with the metrics and metadata required to calculate cost allocation. Prometheus provides longer retention periods and more detailed metrics than the OpenCost Collector, which is useful for historical analysis and cost forecasting.

# Sharded Prometheus Best Practices

**If you are running Prometheus in a sharded (HA) setup:**

- Each Prometheus pod only scrapes a subset of targets. If OpenCost is configured to query a single Prometheus pod, it will only see partial data, and export jobs may fail or return incomplete results.
- To ensure complete and reliable cost data, set `PROMETHEUS_SERVER_ENDPOINT` to a global query endpoint that aggregates all shards, such as [Thanos Query](https://thanos.io/tip/components/query.md/), [Cortex Query Frontend](https://cortexmetrics.io/docs/architecture/), or [Mimir Query Frontend](https://grafana.com/docs/mimir/latest/operations/query-frontend/).
- If you do not use a global endpoint, you may experience intermittent failures or missing data in OpenCost exports.

**Example:**

```
export PROMETHEUS_SERVER_ENDPOINT="http://thanos-query-frontend:9090"
```

For more details, see the [OpenCost documentation](https://www.opencost.io/docs/installation/prometheus) and the documentation for your query aggregator.