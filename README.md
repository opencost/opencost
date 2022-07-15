<img src="./opencost-header.png"/>

# OpenCost — your favorite open source cost monitoring tool for Kubernetes

OpenCost models give teams visibility into current and historical Kubernetes spend and resource allocation. These models provide cost transparency in Kubernetes environments that support multiple applications, teams, departments, etc.

OpenCost was originally developed and [open sourced](https://github.com/opencost/opencost/issues/1224) by [Kubecost](https://kubecost.com). This project combines a [specification](/spec/) as well as a Golang implementation of these detailed requirements.

![OpenCost allocation UI](/allocation-drilldown.gif)

To see the full functionality of OpenCost you can view [OpenCost features](https://opencost.io). Here is a summary of features enabled:

- Real-time cost allocation by Kubernetes service, deployment, namespace, label, statefulset, daemonset, pod, and container
- Dynamic asset pricing enabled by integrations with AWS, Azure, and GCP billing APIs
- Supports on-prem k8s clusters with custom pricing sheets
- Allocation for in-cluster resources like CPU, GPU, memory, and persistent volumes.
- Allocation for AWS & GCP out-of-cluster resources like RDS instances and S3 buckets with key (optional)
- Easily export pricing data to Prometheus with /metrics endpoint ([learn more](PROMETHEUS.md))
- Free and open source distribution (Apache2 license)

## Getting Started

You can deploy OpenCost on any Kubernetes 1.8+ cluster in a matter of minutes, if not seconds!

Visit the full documentation for [recommended install options](https://docs.kubecost.com/install). Compared to building from source, installing from Helm is faster and includes all necessary dependencies.

## Usage

- User interface
- [Cost APIs](https://github.com/kubecost/docs/blob/master/apis.md)
- [CLI / kubectl cost](https://github.com/kubecost/kubectl-cost)
- [Prometheus metric exporter](kubecost-exporter.md)

## Contributing

We :heart: pull requests! See [`CONTRIBUTING.md`](CONTRIBUTING.md) for information on buiding the project from source
and contributing changes.

## Community

If you need any support or have any questions on contributing to the project, you can reach us on [Slack](https://join.slack.com/t/kubecost/shared_invite/enQtNTA2MjQ1NDUyODE5LWFjYzIzNWE4MDkzMmUyZGU4NjkwMzMyMjIyM2E0NGNmYjExZjBiNjk1YzY5ZDI0ZTNhZDg4NjlkMGRkYzFlZTU) or via email at [team@kubecost.com](team@kubecost.com).

## FAQ

### _How do you measure the cost of CPU/RAM/GPU/storage for a container, pod, deployment, etc._

The OpenCost cost model collects pricing data from major cloud providers, e.g. GCP, Azure and AWS, to provide the real-time cost of running workloads. Based on data from these APIs, each container/pod inherits a cost per CPU-hour, GPU-hour, Storage Gb-hour and cost per RAM Gb-hour based on the node where it was running or the class of storage provisioned. This means containers of the same size, as measured by the max of requests or usage, could be charged different resource rates if they are scheduled in separate regions, on nodes with different usage types (on-demand vs preemptible), etc.

For on-prem clusters, these resource prices can be configured directly with custom pricing sheets (more below).

Measuring the CPU/RAM/GPU cost of a deployment, service, namespace, etc is the aggregation of its individual container costs.

### _How do you determine RAM/CPU/GPU costs for a node when this data isn’t provided by a cloud provider?_

When explicit RAM, CPU or GPU prices are not provided by your cloud provider, the OpenCost model falls back to the ratio of base CPU, GPU and RAM price inputs supplied. The default values for these parameters are based on the marginal resource rates of the cloud provider, but they can be customized within OpenCost.

These base RAM/CPU/GPU prices are normalized to ensure the sum of each component is equal to the total price of the node provisioned, based on billing rates from your provider. When the sum of RAM/CPU/GPU costs is greater (or less) than the price of the node, then the ratio between the input prices is held constant.

As an example, let's imagine a node with 1 GPU, 1 CPU and 1 Gb of RAM that costs $35/mo. If your base GPU price is $30, base CPU price is $30 and RAM Gb price is $10, then these inputs will be normalized to $15 for GPU, $15 for CPU and $5 for RAM so that the sum equals the cost of the node. Note that the price of a GPU, as well as the price of a CPU remain 3x the price of a Gb of RAM.

    NodeHourlyCost = NORMALIZED_GPU_PRICE * # of GPUS + NORMALIZED_CPU_PRICE * # of CPUS + NORMALIZED_RAM_PRICE * # of RAM Gb

### _How do you allocate a specific amount of RAM/CPU to an individual pod or container?_

Resources are allocated based on the time-weighted maximum of resource Requests and Usage over the measured period. For example, a pod with no usage and 1 CPU requested for 12 hours out of a 24 hour window would be allocated 12 CPU hours. For pods with BestEffort quality of service (i.e. no requests) allocation is done solely on resource usage.

### _How do I set my AWS Spot estimates for cost allocation?_

Modify [spotCPU](https://github.com/opencost/opencost/blob/master/configs/default.json#L5) and [spotRAM](https://github.com/opencost/opencost/blob/master/configs/default.json#L7) in default.json to the level of recent market prices. Allocation will use these prices, but it does not take into account what you are actually charged by AWS. Alternatively, you can provide an AWS key to allow access to the Spot data feed. This will provide accurate Spot price reconciliation.

### _Do I need a GCP billing API key?_

We supply a global key with a low limit for evaluation, but you will want to supply your own before moving to production.
