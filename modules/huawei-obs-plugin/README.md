# huawei-obs-plugin

An OpenCost [Custom Cost](../../pkg/customcost) plugin that prices Huawei Cloud OBS
(Object Storage Service) bucket storage, using live bucket sizes from the OBS API and
live on-demand pricing from the Huawei Cloud BSS `demandPrice` API.

This is a separate Go module and binary, loaded by OpenCost as a subprocess over
gRPC (via [hashicorp/go-plugin](https://github.com/hashicorp/go-plugin)) — it is not
part of `pkg/cloud/huawei`, which handles ECS/EVS pricing for the cluster's own
nodes/disks. See `HUAWEI_CLOUD_INTEGRATION_PLAN.md` in the repo root for how this
fits into the broader Huawei Cloud integration.

## Building

```bash
cd modules/huawei-obs-plugin
go build -o huaweiobs.ocplugin.<os>.<arch> .
```

`<os>`/`<arch>` must match `runtime.GOOS`/`runtime.GOARCH` of the machine running
OpenCost (e.g. `linux`/`amd64`, `windows`/`amd64`). This naming is required by
OpenCost's plugin loader (`pkg/customcost/pipelineservice.go`).

## Configuring OpenCost to load this plugin

1. Build the binary as above and place it in a directory, e.g. `./plugins/exec/`.
2. Create a config file in a separate directory, e.g. `./plugins/config/huaweiobs_config.json`:

   ```json
   {
     "region": "la-south-2",
     "buckets": []
   }
   ```

   `region` is required — it's both the OBS endpoint region and the region BSS is
   asked to price. `buckets` is optional; if empty (or omitted), every bucket
   visible to the configured credentials is priced.

3. Set these environment variables when starting OpenCost:

   | Variable | Value |
   |---|---|
   | `CUSTOM_COST_ENABLED` | `true` |
   | `PLUGIN_CONFIG_DIR` | `./plugins/config` |
   | `PLUGIN_EXECUTABLE_DIR` | `./plugins/exec` |
   | `HUAWEICLOUD_ACCESS_KEY_ID` | your Huawei Cloud AK |
   | `HUAWEICLOUD_SECRET_ACCESS_KEY` | your Huawei Cloud SK |
   | `HUAWEICLOUD_DOMAIN_ID` | your Huawei Cloud IAM domain ID |
   | `HUAWEICLOUD_PROJECT_ID` | the project ID for the region set in the config file |

   Credentials are read from the environment (inherited by the plugin subprocess),
   not from the config file, so they're never written to disk here — the same
   convention `pkg/cloud/huawei` uses for the main provider.

4. Start OpenCost normally (`go run ./cmd/costmodel/main.go` from the repo root, or
   the built binary). On startup you should see a log line like
   `requiring plugins matching your architecture: amd64` followed by the plugin
   process starting.
5. Check `GET /customCost/status` — the `huaweiobs` domain should appear once the
   first ingest window completes.

## Notes / open questions

- The OBS product/resource codes used to query BSS were confirmed against a live
  account. They are *not* analogous to the ECS/EVS codes: `resource_spec` for OBS is
  not a free-form instance/volume type string, it's one of a fixed set of
  per-storage-class/per-redundancy SKU codes (discovered via
  `GET /v2/products/usage-types?resource_type_code=hws.resource.type.obs`), and the
  `usage_factor`/`usage_measure_id` pair is `"size"`/`9` (a Huawei-internal "division
  measure" enum value) rather than the `"Duration"`/`4` (hour) pair EVS uses. See the
  comment on `obsResourceSpecStandard3AZ` in `pricing.go` for the full explanation.
- Only the OBS Standard storage class with 3-AZ redundancy is priced today
  (`obsResourceSpecStandard3AZ = "stdandard_ext_3az_size_type1"`, note the SKU code's
  own "stdandard" typo is Huawei's, not ours). Other SKUs exist for 1-AZ redundancy
  and for the Warm (infrequent access) and Cold (archive) storage classes (e.g.
  `warm_ext_3az_size_type1`, `cold_ext_1az_size_type1`).
- Bucket size comes from `GetBucketStorageInfo`, which reports the bucket's total
  size across all storage classes (Standard/Warm/Cold) as a single number, but is
  currently priced entirely at the Standard/3AZ rate above. A bucket using Warm/Cold
  storage classes, or 1-AZ redundancy, will be priced inaccurately until this plugin
  splits pricing by tier using the `Standard`/`Warm`/`Cold` breakdown
  `GetBucketStorageInfo` provides and selects the matching SKU code per tier.
