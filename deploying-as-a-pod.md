## Deploying as a pod

See this page for all [Kubecost install options](http://docs.kubecost.com/install).

If you would like to deploy the cost model (w/o dashboards) directly a pod on your cluster, complete the listed below. 

1. Set [this environment variable](https://github.com/kubecost/cost-model/blob/master/kubernetes/deployment.yaml#L30) to the address of your prometheus server
2. `kubectl create namespace cost-model`
3. `kubectl apply -f kubernetes/ --namespace cost-model`
4. `kubectl port-forward --namespace cost-model service/cost-model 9003`

To test that the server is running, you can hit [http://localhost:9003/costDataModel?timeWindow=1d](http://localhost:9003/costDataModel?timeWindow=1d)

**Note:** This approach provides less functionality compared to other install options referenced above. Also, Prometheus and kube-state-metrics are external dependencies for this installation path.
