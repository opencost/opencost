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

## Getting Started

You can deploy Kubecost on any Kubernetes 1.8+ cluster in a matter of minutes, if not seconds. 
Visit the Kubecost docs for [recommended install options](https://docs.kubecost.com/install). Compared to building from source, installing from Helm is faster and includes all necessary dependencies. 

## Contributing

We :heart: pull requests! See [`CONTRIBUTING.md`](CONTRIBUTING.md) for information on buiding the project from source
and contributing changes. 

To test that the server is running, you can hit [http://localhost:9003/costDataModel?timeWindow=1d](http://localhost:9003/costDataModel?timeWindow=1d)

## Licensing

Licensed under the Apache License, Version 2.0 (the "License")

 ## Software stack

Golang application. 
Prometheus. 
Kubernetes. 

## Frequently Asked Questions

***How do you measure the cost of CPU/RAM/GPU/storage for a container, pod, deployment, etc.***

The Kubecost model collects pricing data from major cloud providers, e.g. GCP, Azure and AWS, to provide the real-time cost of running workloads. Based on data from these APIs, each container/pod inherits a cost per CPU-hour, GPU-hour, Storage Gb-hour and cost per RAM Gb-hour based on the node where it was running or the class of storage provisioned. This means containers of the same size, as measured by the max of requests or usage, could be charged different resource rates if they are scheduled in seperate regions, on nodes with different usage types (on-demand vs preemptible), etc. 

For on-prem clusters, these resource prices can be configured directly with custom pricing sheets (more below).

Measuring the CPU/RAM/GPU cost of a deployment, service, namespace, etc is the aggregation of its individual container costs.

***How do you determine RAM/CPU costs for a node when this data isn’t provided by a cloud provider?***

When explicit RAM or CPU prices are not provided by your cloud provider, the Kubecost model falls back to the base CPU and RAM price inputs supplied. These defaults are based on recent GCP rates but can be easily customized via json or in product Settings depending on your installation path. 

These base RAM/CPU prices are normalized to ensure the sum of each component is equal to the total price of the node provisioned, based on billing rates from your provider. When the sum of RAM/CPU costs is greater (or less) than the price of the node, then the ratio between the two input prices are held constant.  

As an example, let's say you have a node with 1 CPU and 1 Gb of RAM that costs $20/mo. If your base CPU price is $30 and your RAM Gb price is $10, then these inputs will be normlized to $15 for CPU and $5 for RAM so that the sum equals the cost of the node. Note that the price of a CPU remains 3x the price of one Gb of RAM. 

    NodeHourlyCost = NORMALIZED_CPU_PRICE * # of CPUS + NORMALIZED_RAM_PRICE * # of RAM Gb

***How do you allocate a specific amount of RAM/CPU to an individual pod or container?***

Resources are allocated based on the time-weighted maximum of resource Requests and Usage over the measured period. For example, a pod with no usage and 1 CPU requested for 12 hours out of a 24 hour window would be allocated 12 CPU hours. For pods with BestEffort quality of service (i.e. no requests) allocation is done solely on resource usage. 

***How do I set my AWS Spot bids for accurate allocation?***

Modify [spotCPU](https://github.com/kubecost/cost-model/blob/master/cloud/default.json#L5) and  [spotRAM](https://github.com/kubecost/cost-model/blob/master/cloud/default.json#L7) in default.json to the price of your bid. Allocation will use these bid prices, but it does not take into account what you are actually charged by AWS. Alternatively, you can provide an AWS key to allow access to the Spot data feed. This will provide accurate Spot prices. 

***Do I need a GCP billing API key?***

We supply a global key with a low limit for evaluation, but you will want to supply your own before moving to production.  
  
Please reach out with any additional questions on  [Slack](https://join.slack.com/t/kubecost/shared_invite/enQtNTA2MjQ1NDUyODE5LTg0MzYyMDIzN2E4M2M5OTE3NjdmODJlNzBjZGY1NjQ3MThlODVjMGY3NWZlNjQ5NjIwNDc2NGU3MWNiM2E5Mjc) or via email at [team@kubecost.com](team@kubecost.com). 
