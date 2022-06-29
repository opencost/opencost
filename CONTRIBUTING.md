# Contributing to our project

Thanks for your help improving the OpenCost project! There are many ways to contribute to the project, including the following:

* contributing or providing feedback on the [OpenCost Spec](https://github.com/kubecost/opencost/tree/develop/spec)
* contributing documentation 
* joining the discussion on [CNCF Slack](https://slack.cncf.io/) in the [#opencost](https://cloud-native.slack.com/archives/C03D56FPD4G) channel or in the [OpenCost community discussions](https://drive.google.com/drive/folders/1hXlcyFPePB7t3z6lyVzdxmdfrbzeT1Jz) folder
* committing software via the workflow below

## Getting Help

If you have a question about OpenCost or have encountered problems using it,
you can start by asking a question on [CNCF Slack](https://slack.cncf.io/) in the [#opencost](https://cloud-native.slack.com/archives/C03D56FPD4G) channel or via email at [support@kubecost.com](support@kubecost.com)

## Workflow

This repository's contribution workflow follows a typical open-source model:

- [Fork](https://docs.github.com/en/get-started/quickstart/fork-a-repo) this repository
- Work on the forked repository
- Open a pull request to [merge the fork back into this repository](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork)

## Building

Follow these steps to build from source and deploy:

1. `docker build --rm -f "Dockerfile" -t <repo>/kubecost-cost-model:<tag> .`
2. Edit the [pulled image](https://github.com/kubecost/opencost/blob/master/kubernetes/deployment.yaml#L25) in the deployment.yaml to <repo>/kubecost-cost-model:<tag>
3. Set [this environment variable](https://github.com/kubecost/opencost/blob/master/kubernetes/deployment.yaml#L33) to the address of your prometheus server
4. `kubectl create namespace cost-model`
5. `kubectl apply -f kubernetes/ --namespace cost-model`
6. `kubectl port-forward --namespace cost-model service/cost-model 9003`

To test, build the cost-model docker container and then push it to a Kubernetes cluster with a running Prometheus.

To confirm that the server is running, you can hit [http://localhost:9003/costDataModel?timeWindow=1d](http://localhost:9003/costDataModel?timeWindow=1d)

## Running locally

To run locally cd into `cmd/costmodel` and `go run main.go`

cost-model requires a connection to Prometheus in order to operate so setting the environment variable `PROMETHEUS_SERVER_ENDPOINT` is required.
In order to expose Prometheus to cost-model it may be required to port-forward using kubectl to your Prometheus endpoint.

For example:

```bash
kubectl port-forward svc/kubecost-prometheus-server 9080:80
```

This would expose Prometheus on port 9080 and allow setting the environment variable as so:

```bash
PROMETHEUS_SERVER_ENDPOINT="http://127.0.0.1:9080"
```

If you want to run with a specific kubeconfig the environment variable `KUBECONFIG_PATH` can be used. cost-model will attempt to connect to your Kubernetes cluster in a similar fashion as kubectl so the env is not required. The order of precedence is `KUBECONFIG_PATH` > default kubeconfig file location ($HOME/.kube/config) > in cluster config

Example:

```bash
export KUBECONFIG_PATH=~/.kube/config
```

There are two more environement variabes recommended to run locally. These should be set as the default file location used is `/var/` which usually requires more permissions than kubecost actually needs to run. They do not need to match but keeping everything together can help cleanup when no longer needed.

```bash
ETL_PATH_PREFIX="/my/cool/path/kubecost/var/config"
CONFIG_PATH="/my/cool/path/kubecost/var/config"
```

An example of the full command:

```bash
ETL_PATH_PREFIX="/my/cool/path/kubecost/var/config" CONFIG_PATH="/my/cool/path/kubecost/var/config" PROMETHEUS_SERVER_ENDPOINT="http://127.0.0.1:9090" go run main.go
```

## Running the integration tests

To run these tests:

- Make sure you have a kubeconfig that can point to your cluster, and have permissions to create/modify a namespace called "test"
- Connect to your the prometheus kubecost emits to on localhost:9003:
  `kubectl port-forward --namespace kubecost service/kubecost-prometheus-server 9003:80`
- Temporary workaround: Copy the default.json file in this project at cloud/default.json to /models/default.json on the machine your test is running on. TODO: fix this and inject the cloud/default.json path into provider.go.
- Navigate to cost-model/test
- Run `go test -timeout 700s` from the testing directory. The tests right now take about 10 minutes (600s) to run because they bring up and down pods and wait for Prometheus to scrape data about them.

## Certification of Origin

By contributing to this project you certify that your contribution was created in whole or in part by you and that you have the right to submit it under the open source license indicated in the project. In other words, please confirm that you, as a contributor, have the legal right to make the contribution.

## Committing

Please write a commit message with Fixes Issue # if there is an outstanding issue that is fixed. It’s okay to submit a PR without a corresponding issue, just please try be detailed in the description about the problem you’re addressing.

Please run go fmt on the project directory. Lint can be okay (for example, comments on exported functions are nice but not required on the server).

Please email us [support@kubecost.com](support@kubecost.com) or reach out to us on [CNCF Slack](https://slack.cncf.io/) in the [#opencost](https://cloud-native.slack.com/archives/C03D56FPD4G) channel if you need help or have any questions!
