# OpenCost UI
The preferred install path for OpenCost is via Helm chart, and is available explained [here](http://github.com/opencost/opencost-helm-chart)

To manually run the OpenCost UI, follow the steps below.

## Requirements

* `nodejs >= 18.3.0`
* `npm >= 8.11.0`

## Installation & Running
To run the UI, open a terminal to the `opencost/ui/` directory (where this README is located) and run

```
npm install
```

This will install required dependencies and build tools. To launch the UI, run

```
npx parcel src/index.html
```

This will launch a development server, serving the UI at `http://localhost:1234` and targeting the data for an instance of
OpenCost running at `http://localhost:9090`. To access an arbitrary OpenCost install, you can use

```
kubectl port-forward deployment/opencost 9090:9003
```

for your choice of namespace and cloud context.
