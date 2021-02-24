# Running Kubecost as a Prometheus metric exporter

Running Kubecost as a Prometheus metric exporter allows you to export various cost metrics to Prometheus without setting up any other Kubecost dependencies. Doing so lets you write PromQL queries to calculate the cost and efficiency of any Kubernetes concept, e.g. namespace, service, label, deployment, etc. You can also calculate the cost of different Kubernetes resources, e.g. nodes, PVs, LoadBalancers, and more. Finally, you can do other interesting things like create custom alerts via AlertManager and custom dashboards via Grafana. 

## Installing

> Note: all deployments of Kubecost function as a Prometheus metric exporter. We strongly recommend helm as an install path to take advantage of Kubecost’s full potential. [View recommended install](http://docs.kubecost.com/install).

If you would prefer to not use the recommended install option and just deploy the Kubecost open source cost model as a metric exporter, you can follow these steps:


1. Apply the combined YAML:

    ```
    kubectl apply -f https://raw.githubusercontent.com/kubecost/cost-model/develop/kubernetes/exporter/exporter.yaml --namespace cost-model
    ```

    > If you want to use a namespace other than `cost-model`, you will have to edit the `ClusterRoleBinding` after applying the YAML to change `subjects[0].namespace`. You can do this with `kubectl edit clusterrolebinding cost-model`.

2. To verify that metrics are available:

    ```
    kubectl port-forward --namespace cost-model service/cost-model 9003
    ```

    Visit [http://localhost:9003/metrics](http://localhost:9003/metrics) to see exported metrics

Add Kubecost scrape config to Prom ([more](https://prometheus.io/docs/introduction/first_steps/#configuring-prometheus))
```
- job_name: cost-model
  scrape_interval: 1m
  scrape_timeout: 10s
  metrics_path: /metrics
  scheme: http
  static_configs:
    - targets: ['cost-model.cost-model.:9003']
```

Done! Kubecost is now exporting cost metrics. See the following sections for different metrics available and query examples.

## Available Prometheus Metrics 

| Metric       | Description                                                                                            |
| ------------ | ------------------------------------------------------------------------------------------------------ |
| node_cpu_hourly_cost | Hourly cost per vCPU on this node  |
| node_gpu_hourly_cost | Hourly cost per GPU on this node  |
| node_ram_hourly_cost   | Hourly cost per Gb of memory on this node                       |
| node_total_hourly_cost   | Total node cost per hour                       |
| container_cpu_allocation   | Average number of CPUs requested over last 1m                      |
| container_memory_allocation_bytes   | Average bytes of RAM requested over last 1m                 |
| kubecost_load_balancer_cost | Hourly cost of load balancers |
| kubecost_cluster_management_cost | Hourly cost of cluster management |


By default, the cost metrics are based on public billing APIs. Supported platforms: AWS, Azure, and GCP. For on-prem clusters, prices are based on configurable defaults. 

More metrics are available in the recommended install path and are described in [PROMETHEUS.md](PROMETHEUS.md).

## Dashboard examples

Here’s an example dashboard using Kubecost metrics: 

![sample dashboard](https://grafana.com/api/dashboards/8670/images/5480/image)

You can find other example dashboards at https://grafana.com/orgs/kubecost

## Example Queries

Once Kubecost’s cost model is running in your cluster and you have added it in your Prometheus scrape configuration, you can hit Prometheus with useful queries like these:

#### Monthly cost of all nodes

```
sum(node_total_hourly_cost) * 730
```

#### Hourly cost of all load balancers broken down by namespace

```
sum(kubecost_load_balancer_cost) by (namespace)
```

#### Monthly rate of each namespace’s CPU request

```
sum(container_cpu_allocation * on (node) group_left node_cpu_hourly_cost) by (namespace) * 730
```

#### Historical memory request spend for all `fluentd` pods in the `kube-system` namespace

```
avg_over_time(container_memory_allocation_bytes{namespace="kube-system",pod=~"fluentd.*"}[1d])
  * on (pod,node) group_left
avg(count_over_time(container_memory_allocation_bytes{namespace="kube-system"}[1d:1m])/60) by (pod,node)
  * on (node) group_left
avg(avg_over_time(node_ram_hourly_cost[1d] )) by (node)
```


## Setting Cost Alerts

Custom cost alerts can be implemented with a set of Prometheus queries and can be used for alerting with AlertManager or Grafana alerts.

TODO: Add examples

## Limitations

Running in exporter-only mode necessarily limits Kubecost functionality. The following limitations of this install method are handled by the [recommended install](http://docs.kubecost.com/install).

- Persistent volume metrics not available (coming soon!)
- For large clusters, these Prometheus queries might not scale well over large time windows. We recommend using Kubecost APIs for these scenarios.
- Allocation metrics, like `container_cpu_allocation` only contain _requests_ and do not take into account usage.
- Efficiency metrics are not available
- Public billing costs on default. The standard Kubecost install + a cloud integration gives you accurate pricing based on your bill. 



