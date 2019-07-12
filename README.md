Kubecost models give teams visibility into current and historical Kubernetes spend and resource allocation. These models  provide cost transparency in Kubernetes environments that support multiple applications, teams, departments, etc.

![Kubecost dashboard](https://github.com/kubecost/cost-model/blob/master/allocation-dashboard.png)

To see more on the functionality of the full Kubecost product, please visit the [features page](https://kubecost.com/#features) on our website. 
Here is a summary of features enabled by this cost model:

- Real-time cost allocation for native Kubernetes concepts: service, deployment, namespace, label, daemonset, pod, container, and more
- Dynamic asset pricing enabled by integrations with AWS, Azure and GCP billing APIs 
- Supports on-prem k8s clusters with custom pricing sheets
- Allocation for in-cluster resources like CPU, GPU, memory, and persistent volumes.
- Allocation for AWS & GCP out-of-cluster resources like RDS instances and S3 buckets with key (optional)
- Easily export pricing data to Prometheus with /metrics endpoint ([learn more](PROMETHEUS.md))
- Free and open source distribution (Apache2 license)

## Requirements

- Kubernetes version 1.8 or higher
- kube-state-metrics
- Node exporter
- Prometheus

## Installation

You can run Kubecost models on any Kubernetes 1.8+ cluster in a matter of minutes, if not seconds. 
The recommended way to install the Kubecost cost model (along with dashboards) is with the [install instructions](https://kubecost.com/install) on our website. Note: this installation also contains closed source dashboards today, but they are provided for free on small clusters and for evaluation purposes. 

Compared to building from source, this is faster and includes all necessary dependencies. The Kubecost cost model is also available as a container (kubecost/cost-model) and you can deploy run the Golang code yourself as described in the section below.

## Deploying as a pod

If you would like to run the cost model (w/o dashboards mentioned above) directly on your cluster, complete the following steps:

1. Set [this environment variable](https://github.com/kubecost/cost-model/blob/master/kubernetes/deployment.yaml#L30) to the address of your prometheus server
2. `kubectl create namespace cost-model`
3. `kubectl apply -f kubernetes/ --namespace cost-model`
4. `kubectl port-forward --namespace cost-model service/cost-model 9003`

To test that the server is running, you can hit [http://localhost:9003/costDataModel?timeWindow=1d](http://localhost:9003/costDataModel?timeWindow=1d)

Note: the following dependencies mentioned above are required for this installation path.

## Contributing

We :heart: pull requests! See [`CONTRIBUTING.md`](CONTRIBUTING.md) for info on
contributing changes. To test, you'll need to build the cost-model docker container then push it to a kubernetes cluster with a running prometheus.

1. `docker build --rm -f "Dockerfile" -t <repo>/kubecost-cost-model:<tag> .`
2. Edit the [pulled image](https://github.com/kubecost/cost-model/blob/master/kubernetes/deployment.yaml#L22) in the deployment.yaml to <repo>/kubecost-cost-model:<tag>
3. Set [this environment variable](https://github.com/kubecost/cost-model/blob/master/kubernetes/deployment.yaml#L30) to the address of your prometheus server
4. `kubectl create namespace cost-model`
5. `kubectl apply -f kubernetes/ --namespace cost-model`
6. `kubectl port-forward --namespace cost-model service/cost-model 9003`

## Licensing

Licensed under the Apache License, Version 2.0 (the "License")

 ## Software stack

Golang application. 
Prometheus. 
Kubernetes. 

## Questions

***How do you measure the cost of a CPU/RAM/GPU for a container, pod, deployment, etc.***

The Kubecost model collects pricing data from major cloud providers, e.g. GCP, Azure and AWS, to provide the real-time cost of running workloads. Based on data from these APIs, each container/pod inherits a cost per CPU hour, GPU hour, and cost per RAM Gb hour based on the node where it was running. This means containers of the same size, as measured by the max of requests or usage, could be charged different resource rates if they are scheduled in seperate regions, on nodes with different usage types (on-demand vs preemptible), etc. 

For on-prem clusters, these resource prices can be configred directly with custom pricing sheetsc (more below).

Measuring the CPU/RAM/GPU cost of a deployment, service, namespace, etc is the aggregation of its individual container costs.

***How do you determine RAM/CPU costs for a node when this data isn’t provided by a cloud provider?***

The Kubecost model starts by pegging to a default CPU price when this information is not supplied by a cloud provider or when Kubecost is deployed into an on-prem cluster. This CPU price value is configuable and supplied via json. The price of RAM is then determined by the total node cost less the combined price of all CPUs (i.e. # of CPUs attached to the node multiplied by the per CPU price). The value of both are then normalized to ensure RAM + CPU costs are never greater than the total price of the node when a cloud provider is able to provide total node cost.

    CPUHourlyCost = CONFIGURABLE_CPU_PRICE (if not directly supplied by Cloud provider)

    RAMGBHourlyCost = TOTAL_NODE_COST - CPUHourlyCost * # of CPUS

***How do you allocate a specific amount of RAM/CPU to an individual pod or container?***

Resources are allocated based on the time-weighted maximum of resource Requests and Usage over the measured period. For example, a pod with no usage and 1 CPU requested for 12 hours out of a 24 hour window would be allocated 12 CPU hours. For pods with BestEffort quality of service (i.e. no requests) allocation is done solely on resource usage. 

***How do I set my AWS spot bids for allocation?***

Modify [spotCPU](https://github.com/kubecost/cost-model/blob/master/cloud/default.json#L5) and  [spotRAM](https://github.com/kubecost/cost-model/blob/master/cloud/default.json#L7) in default.json to the price of your bid. Allocation will use these bid prices, but does not take into account what you are actually charged.

***Do I need a GCP billing API key?***

We supply a global key with a low limit for evaluation, but you will want to supply your own before moving to production.
  
  
Please reach out with any additional questions on  [Slack](https://join.slack.com/t/kubecost/shared_invite/enQtNTA2MjQ1NDUyODE5LTg0MzYyMDIzN2E4M2M5OTE3NjdmODJlNzBjZGY1NjQ3MThlODVjMGY3NWZlNjQ5NjIwNDc2NGU3MWNiM2E5Mjc) or via email at [team@kubecost.com](team@kubecost.com). 
