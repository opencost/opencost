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

The UI can be run locally using the `npm run serve` command.

```sh
$ npm run serve
> kubecost-ui-open@0.0.1 serve
> npx parcel serve src/index.html

Server running at http://localhost:1234
✨ Built in 1.96s
```

And can have a custom URL backend prefix.

```sh
BASE_URL=http://localhost:9090/test npm run serve

> kubecost-ui-open@0.0.1 serve
> npx parcel serve src/index.html

Server running at http://localhost:1234
✨ Built in 772ms
```

In addition, similar behavior can be replicated with the docker container:

```sh
$ docker run -e BASE_URL_OVERRIDE=test -p 9091:9090 -d opencost-ui:latest
$ curl localhost:9091
<html gibberish> 
```

## Overriding the Base API URL

For some use cases such as the case of [Opencost deployed behind an ingress controller](https://github.com/opencost/opencost/issues/1677), it is useful to override the `BASE_URL` variable responsible for requests sent from the UI to the API.  This means that instead of sending requests to `<domain>/model/allocation/compute/etc`, requests can be sent to `<domain>/{BASE_URL_OVERRIDE}/allocation/compute/etc`.  To do this, supply the environment variable `BASE_URL_OVERRIDE` to the docker image.

```sh
$ docker run -p 9091:9090 -e BASE_URL_OVERRIDE=anything -d opencost-ui:latest
```