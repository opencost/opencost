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

The provider can also match a node using `spec.providerID`,
`metadata.labels.<label-name>`, or `metadata.annotations.<annotation-name>` as
the `InstanceIDField`. The `InstanceID` column must contain the corresponding
value from each node. See
[`NodeValueFromMapField`](../pkg/cloud/provider/csvprovider.go) for the exact
field handling.

## Enable the provider

Make the CSV file readable inside the OpenCost container, for example by
mounting it from a ConfigMap, and set these environment variables on that
container:

```text
USE_CSV_PROVIDER=true
CSV_PATH=/etc/opencost/pricing.csv
```

`CSV_PATH` is the **path inside the container**, not the path on the machine
where you create the file. Point it to the mounted CSV file. The CSV provider
is selected when `USE_CSV_PROVIDER` is true; no separate custom-provider flag
is needed.

After deployment, check the OpenCost logs for `Using CSV Provider with CSV at`
and confirm the path is the one you mounted. Query node assets through the
[`/assets` API](https://www.opencost.io/docs/integrations/api) to check the
resulting costs. The provider reloads the file every 60 minutes, so a changed
ConfigMap may not be reflected immediately.

The CSV provider also supports GPU and persistent-volume rows. See the
[sample CSV](../configs/pricing_schema.csv) and
[`CSVProvider`](../pkg/cloud/provider/csvprovider.go) for their field mapping.
