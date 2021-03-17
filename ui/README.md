# Kubecost Open Source UI

## Installation & Running
To run the UI, open a terminal to the `cost-model/ui/` directory (where this README is located) and run

```
npm install
```

This will install required depndencies and build tools. To launch the UI, run

```
npx parcel src/index.html
```

This will launch a development server, serving the UI at `http://localhost:1234` and targeting the data for an instance of
Kubecost running at `http://localhost:9090`. To access an arbitrary Kubecost install, you can use

```
kubectl port-forward deployment/kubecost-cost-analyzer 9090
```

for your choice of namespace and cloud context.
