# Exporter Deployment

This is the one YAML file that is the aggregation of the regular deployment files. This is done for easy distribution, allowing users to `kubectl apply` an exporter-only deployment without cloning the whole repository. Apply on the parent directory won't apply anything in this directory unless `kubectl apply --recursive=True` is used.

## Usage

Please be aware, you will have to change both the `Namespace` and `ClusterRoleBinding` resource if you want to deploy to a namespace other than `cost-model`.

``` sh
kubectl apply -f exporter.yaml --namespace cost-model
```
