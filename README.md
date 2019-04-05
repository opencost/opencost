Kubecost models give teams visibility into current and historical Kubernetes spend and resource allocation. These models  provide cost transparency in Kubernetes environments that support multiple applications, teams, departments, etc.

![Kubecost dashboard](https://github.com/kubecost/cost-model/blob/master/allocation-dashboard.png)

To see more on the functionality of the full Kubecost product, please visit the [features page](https://app.kubemonitor.com/home.html#features) on our website. 
Here is a summary of features enabled by this cost model:

- Real-time cost allocation for native Kubernetes concepts: service, deployment, namespace, label, daemonset, pod, container, and more
- Dynamic asset pricing enabled by integrations with AWS and GCP billing APIs, estimates for Azure 
- Supports on-prem k8s clusters with custom pricing sheets
- Allocation for in-cluster resources like CPU, GPU, memory, and persistent volumes.
- Allocation for out-of-cluster resources like RDS instances and S3 buckets with key (AWS only today)
- Free and open source distribution (Apache2 license)

## Requirements

- Kubernetes version 1.8 or higher
- kube-state-metrics
- Node exporter
- Prometheus

## Installation

You can run Kubecost models on any Kubernetes 1.8+ cluster in a matter of minutes, if not seconds. 
The recommended way to install the Kubecost cost model (along with dashboards) is with the [Helm chart install](https://app.kubemonitor.com/install) on our website. Note that this Helm installation also contains closed source dashboards today, but they are provided free for evaluation purposes. 

Compared to building from source, this is faster and includes all necessary dependencies. The Kubecost cost model is also available as a container (kubecost/cost-model) and you can deploy run the Golang code yourself as described in the section below.

## Contributing

We welcome any contributions to the project! 

If you want to just run the cost model (w/o dashboards) directly on your cluster, complete the following steps:

1. Set [this environment variable](https://github.com/kubecost/cost-model/blob/master/kubernetes/deployment.yaml#L30) to the address of your prometheus server
2. `kubectl create namespace cost-model`
3. `kubectl apply -f kubernetes/ --namespace cost-model`
4. `kubectl port-forward --namespace cost-model service/cost-model 9001`

To test that the server is running, you can hit http://localhost:9001/costDataModel?timeWindow=1d

Note: the following dependencies mentioned above are required for this installation path.


## Licensing

Licensed under the Apache License, Version 2.0 (the "License")

 ## Software stack

Golang application. 
Prometheus. 
Kubernetes. 

## Questions

***How do we allocate RAM/CPU costs for a node when this data isn’t provided by a cloud provider (e.g. EC2 worker nodes)?***

Kubecost models start by pegging to a CPU price for this machine (currently depends on spot/preemptible vs ondemand). This CPU price value is also customizable. The price of RAM is then determined by the total node cost less the total price of all CPUs (i.e. # of CPUs attached to the node multiplied by the per CPU price). The value of both are then normalized to ensure RAM + CPU costs are never greater than the total price of the node.

    CPUHourlyCost = CONFIGURABLE_CPU_PRICE (if not directly supplied by Cloud provider)

    RAMGBHourlyCost = TOTAL_NODE_COST - CPUHourlyCost * # of CPUS

***How do we allocate RAM/CPU to an individual pod or container?***

Resources are allocated based on the maximum of Request and Usage time-weighted for the measured period. For pods with BestEffort quality of service (i.e. no requests) allocation is done solely on resource usage. 

***How do I set my AWS spot bids for allocation?***

Modify [spotCPU](https://github.com/kubecost/cost-model/blob/master/cloud/default.json#L5) and  [spotRAM](https://github.com/kubecost/cost-model/blob/master/cloud/default.json#L7) in default.json to the price of your bid. Allocation will use these bid prices, but does not take into account what you are actually charged.

***Do I need a GCP billing API key?***

We supply a global key with a low limit for evaluation, but you will want to supply your own before moving to production.
  
  
Please reach out with any additional questions on  [Slack](https://join.slack.com/t/kubecost/shared_invite/enQtNTA2MjQ1NDUyODE5LTg0MzYyMDIzN2E4M2M5OTE3NjdmODJlNzBjZGY1NjQ3MThlODVjMGY3NWZlNjQ5NjIwNDc2NGU3MWNiM2E5Mjc) or via email at [team@kubecost.com](team@kubecost.com). 
