# OpenCost UI

## Installing

See https://www.opencost.io/docs/install for the full instructions.

```
helm install my-prometheus --repo https://prometheus-community.github.io/helm-charts prometheus \
  --namespace prometheus --create-namespace \
  --set pushgateway.enabled=false \
  --set alertmanager.enabled=false \
  -f https://raw.githubusercontent.com/opencost/opencost/develop/kubernetes/prometheus/extraScrapeConfigs.yaml

kubectl apply --namespace opencost -f https://raw.githubusercontent.com/opencost/opencost/develop/kubernetes/opencost.yaml
```

## Using

After following the installation instructions, access the UI by port forwarding:
```
kubectl port-forward --namespace opencost service/opencost 9090
```
