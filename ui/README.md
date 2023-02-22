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

## Running Locally

The UI can be run locally using the `make` command.

```sh
$ make serve
BASE_URL="/model" npx parcel serve src/index.html
Server running at http://localhost:1234
✨ Built in 1.04s
```

And can have a custom URL backend prefix.

```sh
$ make serve BASE_URL=test
BASE_URL=test npx parcel serve src/index.html
Server running at http://localhost:1234
✨ Built in 746ms
```

In addition, similar behavior can be replicated with the docker container:

```sh
$ docker run -e BASE_URL_OVERRIDE=test -p 9091:9090 -d opencost-ui:latest
$ curl localhost:9091
<html gibberish> 
```