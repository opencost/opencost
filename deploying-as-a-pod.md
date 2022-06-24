# Deploying as a pod

To deploy OpenCost directly, complete the steps listed below.

1. Set [this environment variable](https://github.com/kubecost/opencost/blob/c211fbc1244a9da9667c7180a9e4c7f988d7978a/kubernetes/deployment.yaml#L33) to the address of your prometheus server
2. `kubectl create namespace cost-model`
3. `kubectl apply -f kubernetes/ --namespace cost-model`
4. `kubectl port-forward --namespace cost-model service/cost-model 9003`

To test that the server is running, you can hit [http://localhost:9003/costDataModel?timeWindow=1d](http://localhost:9003/costDataModel?timeWindow=1d)

**Note:** This approach provides less functionality compared to other install options referenced above. Also, Prometheus and kube-state-metrics are external dependencies for this installation path.
