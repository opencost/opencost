# CSV pricing for Kubernetes nodes

OpenCost can read hourly node prices from a CSV file. This is useful when the
default cloud provider pricing does not represent your node costs, such as for
on-premises clusters or unsupported cloud providers.

## Create a pricing file

The CSV must use the column names in
[`configs/pricing_schema.csv`](../configs/pricing_schema.csv). For example, to
price two nodes by their Kubernetes `metadata.name`:

```csv
EndTimestamp,InstanceID,Region,AssetClass,InstanceIDField,InstanceType,MarketPriceHourly,Version
,worker-a,,node,metadata.name,,0.24,
,worker-b,,node,metadata.name,,0.48,
```

Replace `worker-a` and `worker-b` with the names returned by `kubectl get
nodes`. Each `MarketPriceHourly` value is the hourly price for that node. Keep
`AssetClass` set to `node` and use the same `InstanceIDField` for all node
rows. The example leaves optional metadata columns empty. Node names are
matched without regard to letter case.

For node rows, `InstanceIDField` can be `metadata.name`, `spec.providerID`,
`metadata.labels.<label-name>`, or `metadata.annotations.<annotation-name>`.
The `InstanceID` column must contain the corresponding value from each node.
For `spec.providerID`, the provider extracts the instance ID from AWS provider
IDs and removes the `azure://` prefix from Azure provider IDs; other provider
IDs are used as-is. Use the same `InstanceIDField` for all node rows because
the provider uses one field to look up nodes.

The CSV can also price GPUs and persistent volumes. Use `gpu` for a GPU type:

```csv
EndTimestamp,InstanceID,Region,AssetClass,InstanceIDField,InstanceType,MarketPriceHourly,Version
,Quadro_RTX_4000,,gpu,nvidia.com/gpu_type,,0.75,
```

Here, `InstanceIDField` is a **node label key** and `InstanceID` is its value.
The hourly price is per GPU; OpenCost multiplies it by the node's
`nvidia.com/gpu` capacity and adds it to the node price. A `gpulabel` row uses
the same label key and value but provides a GPU price through the separate
GPU-label lookup.

Use `pv` to price a persistent volume, for example by its name:

```csv
EndTimestamp,InstanceID,Region,AssetClass,InstanceIDField,InstanceType,MarketPriceHourly,Version
,data-pv,,pv,metadata.name,,0.10,
```

For PV rows, `InstanceIDField` can be `metadata.name`,
`metadata.labels.<key>`, `metadata.annotations.<key>`,
`spec.capacity.storage`, or `spec.storageClassName`. Label and annotation keys
with periods are not supported by the current PV lookup. Keep `Region` empty
for GPU and PV rows, and use one `InstanceIDField` for all PV rows.

## Enable the provider

Make the CSV file readable inside the OpenCost container and set these
environment variables on that container:

```text
USE_CSV_PROVIDER=true
CSV_PATH=/etc/opencost/pricing.csv
```

`CSV_PATH` is the **path inside the container**, not the path on the machine
where you create the file. Point it to the mounted CSV file. The CSV provider
is selected when `USE_CSV_PROVIDER` is true; no separate custom-provider flag
is needed.

### Helm chart example

If you install OpenCost with the
[OpenCost Helm chart](https://github.com/opencost/opencost-helm-chart), create
a ConfigMap containing your `pricing.csv` in the release namespace:

```sh
kubectl -n opencost create configmap opencost-csv-pricing \
  --from-file=pricing.csv=pricing.csv
```

Then add the following to your Helm values file:

```yaml
opencost:
  exporter:
    extraEnv:
      USE_CSV_PROVIDER: "true"
      CSV_PATH: /etc/opencost/pricing.csv
    extraVolumeMounts:
      - name: csv-pricing
        mountPath: /etc/opencost
        readOnly: true

extraVolumes:
  - name: csv-pricing
    configMap:
      name: opencost-csv-pricing
```

Install or upgrade the chart in the same namespace as the ConfigMap. The
chart's `opencost.exporter.csv_path` value sets `EXPORT_CSV_FILE` for cost
export; it does **not** set `CSV_PATH` for pricing.

After deployment, check the OpenCost logs for `Using CSV Provider with CSV at`
and confirm the path is the one you mounted. Query node assets through the
[`/assets` API](https://www.opencost.io/docs/integrations/api) to check the
resulting costs. The provider reloads the file every 60 minutes, so a changed
ConfigMap may not be reflected immediately.

The [sample CSV](../configs/pricing_schema.csv) includes additional node and
GPU rows.
