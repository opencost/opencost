# Contributing to our project #

Thanks for your help improving the project!

## Getting Help ##

If you have a question about Kubecost or have encountered problems using it,
you can start by asking a question on [Slack](https://join.slack.com/t/kubecost/shared_invite/enQtNTA2MjQ1NDUyODE5LWFjYzIzNWE4MDkzMmUyZGU4NjkwMzMyMjIyM2E0NGNmYjExZjBiNjk1YzY5ZDI0ZTNhZDg4NjlkMGRkYzFlZTU) or via email at [support@kubecost.com](support@kubecost.com)


## Workflow ##

This repository's contribution workflow follows a typical open-source model:
- [Fork](https://docs.github.com/en/get-started/quickstart/fork-a-repo) this repository
- Work on the forked repository
- Open a pull request to [merge the fork back into this repository](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork)


## Building ## 

Follow these steps to build from source and deploy:

1. `docker build --rm -f "Dockerfile" -t <repo>/kubecost-cost-model:<tag> .`
2. Edit the [pulled image](https://github.com/kubecost/cost-model/blob/master/kubernetes/deployment.yaml#L25) in the deployment.yaml to <repo>/kubecost-cost-model:<tag>
3. Set [this environment variable](https://github.com/kubecost/cost-model/blob/master/kubernetes/deployment.yaml#L33) to the address of your prometheus server
4. `kubectl create namespace cost-model`
5. `kubectl apply -f kubernetes/ --namespace cost-model`
6. `kubectl port-forward --namespace cost-model service/cost-model 9003`

To test, build the cost-model docker container and then push it to a Kubernetes cluster with a running Prometheus.

To confirm that the server is running, you can hit [http://localhost:9003/costDataModel?timeWindow=1d](http://localhost:9003/costDataModel?timeWindow=1d)

## Running locally ##

In order to run cost-model locally, or outside of the runtime of a Kubernetes cluster, you can set the environment variable `KUBECONFIG_PATH`.

Example:
```bash
export KUBECONFIG_PATH=~/.kube/config
```

## Running the integration tests ##
To run these tests:
* Make sure you have a kubeconfig that can point to your cluster, and have permissions to create/modify a namespace called "test"
* Connect to your the prometheus kubecost emits to on localhost:9003: 
```kubectl port-forward --namespace kubecost service/kubecost-prometheus-server 9003:80```
* Temporary workaround: Copy the default.json file in this project at cloud/default.json to /models/default.json on the machine your test is running on. TODO: fix this and inject the cloud/default.json path into provider.go.
* Navigate to cost-model/test
* Run ```go test -timeout 700s``` from the testing directory. The tests right now take about 10 minutes (600s) to run because they bring up and down pods and wait for Prometheus to scrape data about them.


## Certification of Origin ##

By contributing to this project you certify that your contribution was created in whole or in part by you and that you have the right to submit it under the open source license indicated in the project. In other words, please confirm that you, as a contributor, have the legal right to make the contribution. 

## Committing ###

Please write a commit message with Fixes Issue # if there is an outstanding issue that is fixed. It’s okay to submit a PR without a corresponding issue, just please try be detailed in the description about the problem you’re addressing.

Please run go fmt on the project directory. Lint can be okay (for example, comments on exported functions are nice but not required on the server). 

Please email us (support@kubecost.com) or reach out to us on [Slack](https://join.slack.com/t/kubecost/shared_invite/enQtNTA2MjQ1NDUyODE5LWFjYzIzNWE4MDkzMmUyZGU4NjkwMzMyMjIyM2E0NGNmYjExZjBiNjk1YzY5ZDI0ZTNhZDg4NjlkMGRkYzFlZTU) if you need help or have any questions!
