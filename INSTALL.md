# Installation

> If you want Kubecost's full functionality, consider following our standard [installation instructions](https:/docs.kubecost.com/install). This is the fastest way to see Kubecost's potential with more performant queries and a featureful UI. The following instructions are for deployments of _only_ the cost-model repository.

Kubecost's open source `cost-model` can be deployed in two different ways: as only a Prometheus exporter or as both a Prometheus exporter and consumer.

## Standard Install

The standard installation requires Prometheus and [kube-state-metrics](https://github.com/kubernetes/kube-state-metrics) to be running.

> If you wish to deploy in a namespace other than `cost-model`, be sure to change the namespace in the `apply` command _and_ edit the `ClusterRoleBinding` to bind the service in your chosen namespace.

1. Set [this environment variable](https://github.com/kubecost/cost-model/blob/master/kubernetes/deployment.yaml#L33) to the address of your prometheus server
2. `kubectl create namespace cost-model`
3. `kubectl apply -f kubernetes/ --namespace cost-model`


Access the deployment by running `kubectl port-forward --namespace cost-model service/cost-model 9003`. You will have [http://localhost:9003/metrics](http://localhost:9003/metrics) which contains the exported Prometheus metrics as well as the rest of the API available in places like [http://localhost:9003/costDataModel?timeWindow=1d](http://localhost:9003/costDataModel?timeWindow=1d).


## Exporter Only (no Prometheus dependency)

Follow the Standard Install instructions, but skip setting the environment variable. In this configuration, `container_allocation...` metrics will ONLY contain requests, not usage, because usage information requires a configured Prometheus. Additionally, the API provided as part of `cost-model` will not function correctly because it relies on Prometheus.
