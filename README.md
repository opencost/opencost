![OpenCost](./opencost-header.png)

# OpenCost - Your Trusted Open Source Cost Monitoring Tool for Kubernetes

OpenCost empowers teams with invaluable insights into their current and historical Kubernetes spending and resource allocation. These insights foster financial transparency in Kubernetes environments that cater to diverse applications, teams, and departments.

Originally developed and open-sourced by [Kubecost](https://kubecost.com), OpenCost comprises a comprehensive specification and a Golang implementation that meticulously fulfills these requirements.

![OpenCost Allocation User Interface](./ui/src/opencost-ui.png)

For a deep dive into OpenCost's capabilities, explore [OpenCost Features](https://opencost.io). Here's a quick overview of what you can achieve:

- Real-time cost allocation by Kubernetes cluster, node, namespace, controller kind, controller, service, or pod.
- Dynamic on-demand asset pricing, thanks to integrations with AWS, Azure, and GCP billing APIs.
- Full support for on-premises Kubernetes clusters with customizable CSV pricing.
- Allocation for in-cluster resources such as CPU, GPU, memory, and persistent volumes.
- Seamlessly export pricing data to Prometheus via the `/metrics` endpoint (learn more [here](https://www.opencost.io/docs/installation/prometheus)).
- Completely free and open-source distribution under the Apache 2.0 license.

## Getting Started

You can deploy OpenCost on any Kubernetes 1.8+ cluster in just a few minutes, if not seconds!

For the recommended installation options, consult the full documentation at [Installation Guide](https://www.opencost.io/docs/installation/install).

## Usage

- Explore [Cost APIs](https://www.opencost.io/docs/integrations/api).
- Harness the power of [CLI / kubectl cost](https://www.opencost.io/docs/integrations/kubectl-cost).
- Dive into [Prometheus Metrics](https://www.opencost.io/docs/integrations/prometheus).
- Find insights from the [User Interface](https://github.com/opencost/opencost/tree/develop/ui).

## Contributing

We ❤️ pull requests! Refer to the [`CONTRIBUTING.md`](CONTRIBUTING.md) file for guidance on building the project from source and contributing your valuable changes.

## Community

For support or questions about contributing to the project, you can reach out to us on [CNCF Slack](https://slack.cncf.io/) in the [#opencost](https://cloud-native.slack.com/archives/C03D56FPD4G) channel. Join us in the biweekly [OpenCost Working Group community meeting](https://bit.ly/opencost-meeting) via the [Community Calendar](https://bit.ly/opencost-calendar) to engage in OpenCost development discussions.

## FAQ

For answers to common questions, browse our [OpenCost documentation](https://www.opencost.io/docs/FAQ).
