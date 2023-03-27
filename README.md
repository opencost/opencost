<img src="./opencost-header.png"/>

# OpenCost — your favorite open source cost monitoring tool for Kubernetes

OpenCost models give teams visibility into current and historical Kubernetes spend and resource allocation. These models provide cost transparency in Kubernetes environments that support multiple applications, teams, departments, etc.

OpenCost was originally developed and open sourced by [Kubecost](https://kubecost.com). This project combines a [specification](/spec/) as well as a Golang implementation of these detailed requirements.

![OpenCost allocation UI](./ui/src/opencost-ui.png)

To see the full functionality of OpenCost you can view [OpenCost features](https://opencost.io). Here is a summary of features enabled:

- Real-time cost allocation by Kubernetes cluster, node, namespace, controller kind, controller, service, or pod
- Dynamic onDemand asset pricing enabled by integrations with AWS, Azure, and GCP billing APIs
- Supports on-prem k8s clusters with custom CSV pricing
- Allocation for in-cluster resources like CPU, GPU, memory, and persistent volumes.
- Easily export pricing data to Prometheus with /metrics endpoint ([learn more](PROMETHEUS.md))
- Free and open source distribution (Apache2 license)

## Getting Started

You can deploy OpenCost on any Kubernetes 1.8+ cluster in a matter of minutes, if not seconds!

Visit the full documentation for [recommended install options](https://www.opencost.io/docs/install).

## Usage

- [Cost APIs](https://www.opencost.io/docs/api)
- [CLI / kubectl cost](https://www.opencost.io/docs/kubectl-cost)
- [Prometheus Metrics](https://www.opencost.io/docs/prometheus)
- Reference [User Interface](https://github.com/opencost/opencost/tree/develop/ui)

## Contributing

We :heart: pull requests! See [`CONTRIBUTING.md`](CONTRIBUTING.md) for information on building the project from source
and contributing changes.

## Community

If you need any support or have any questions on contributing to the project, you can reach us on [CNCF Slack](https://slack.cncf.io/) in the [#opencost](https://cloud-native.slack.com/archives/C03D56FPD4G) channel, email at [opencost@kubecost.com](opencost@kubecost.com), or attend the [OpenCost Working Group meetings](https://bit.ly/opencost-meeting) from the [Community Calendar](https://bit.ly/opencost-calendar).

## FAQ

You can view [OpenCost documentation](https://www.opencost.io/docs/FAQ) for a list of commonly asked questions.
