# OpenCost Install

## Prerequisites

Install Prometheus:

```yaml
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm install my-prometheus prometheus-community/prometheus --namespace prom --create-namespace -f ./docs/extraScrapeConfigs.yaml
```

## OpenCost

If providing your own Prometheus, set [this environment variable](https://github.com/kubecost/opencost/blob/c211fbc1244a9da9667c7180a9e4c7f988d7978a/kubernetes/deployment.yaml#L33) to the address of your prometheus server

If you used the command from above, this will install OpenCost on your cluster with no edits:

```sh
kubectl create namespace cost-model
kubectl apply -f ./kubernetes/ --namespace cost-model
kubectl port-forward --namespace cost-model service/cost-model 9003
```

## Testing

To test that the server is running, you can hit [http://localhost:9003/costDataModel?timeWindow=1d](http://localhost:9003/costDataModel?timeWindow=1d)

Or use [kubectl cost](https://github.com/kubecost/kubectl-cost):

```sh
kubectl cost --service-port 9003 --service-name cost-model --kubecost-namespace cost-model --allocation-path /allocation/compute  \
    namespace \
    --historical \
    --window 1h 
```

Output:

```sh
+---------+---------------+------------------+-----------------+
| CLUSTER | NAMESPACE     | TOTAL COST (ALL) | COST EFFICIENCY |
+---------+---------------+------------------+-----------------+
|         | prom          |         0.005417 |        0.000000 |
|         | kube-system   |         0.003743 |        0.057180 |
|         | cost-model    |         0.000092 |        0.225071 |
|         | node-exporter |         0.000000 |        0.000000 |
+---------+---------------+------------------+-----------------+
| SUMMED  |               |         0.009252 |                 |
+---------+---------------+------------------+-----------------+
```

Other sample queries: [sample-queries.md](./sample-queries.md)

## Troubleshooting

If you get an error like this, check your prometheus target is correct in the OpenCost deployment.

```bash
Error: failed to query allocation API: failed to port forward query: received non-200 status code 500 and data: {"code":500,"status":"","data":null,"message":"Error: error computing allocation for ...
```

---

## Help

Please let us know if you run into any issues, we are here to help!

[Slack community](https://join.slack.com/t/kubecost/shared_invite/enQtNTA2MjQ1NDUyODE5LWFjYzIzNWE4MDkzMmUyZGU4NjkwMzMyMjIyM2E0NGNmYjExZjBiNjk1YzY5ZDI0ZTNhZDg4NjlkMGRkYzFlZTU)