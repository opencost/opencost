# Hetzner Cloud Cost Tracking with OpenCost

This guide explains how to configure OpenCost to track costs for Hetzner Cloud Kubernetes clusters using the CSV provider.

## Overview

Hetzner Cloud doesn't have native OpenCost integration, but you can use the **CSV Provider** to define custom pricing based on Hetzner's hourly rates. This works with clusters managed by:
- [hcloud-cloud-controller-manager](https://github.com/hetznercloud/hcloud-cloud-controller-manager)
- [hetzner-k3s](https://github.com/vitobotta/hetzner-k3s)
- Manual Kubernetes installations on Hetzner

## Prerequisites

- OpenCost deployed in your Hetzner Kubernetes cluster
- Access to modify OpenCost's configuration

## Node Labels

The Hetzner Cloud Controller Manager automatically adds these labels to nodes:

| Label | Description | Example |
|-------|-------------|---------|
| `node.kubernetes.io/instance-type` | Server type (SKU) | `cpx21` |
| `topology.kubernetes.io/region` | Datacenter region | `fsn1` |
| `topology.kubernetes.io/zone` | Datacenter zone | `fsn1-dc14` |

For Load Balancers (Services of type LoadBalancer):

| Label | Description | Example |
|-------|-------------|---------|
| `load-balancer.hetzner.cloud/type` | Load balancer type | `lb11` |
| `load-balancer.hetzner.cloud/location` | Location | `nbg1` |

## Setup Instructions

### Step 1: Deploy the Pricing CSV

Copy the `hetzner_pricing.csv` file to a location accessible by OpenCost:

**Option A: ConfigMap (Recommended)**

```bash
kubectl create configmap hetzner-pricing \
  --from-file=pricing.csv=configs/hetzner_pricing.csv \
  -n opencost
```

**Option B: S3/Object Storage**

Upload to an S3-compatible bucket (Hetzner Object Storage works too):

```bash
# Using AWS CLI with Hetzner Object Storage
aws s3 cp configs/hetzner_pricing.csv s3://your-bucket/pricing.csv \
  --endpoint-url https://fsn1.your-objectstorage.hetzner.com
```

### Step 2: Configure OpenCost

Add these environment variables to your OpenCost deployment:

```yaml
env:
  - name: USE_CSV_PROVIDER
    value: "true"
  - name: CSV_PATH
    value: "/config/pricing.csv"  # or s3://bucket/pricing.csv
  # For S3:
  # - name: CSV_REGION
  #   value: "fsn1"
  # - name: CSV_ENDPOINT
  #   value: "https://fsn1.your-objectstorage.hetzner.com"
```

Mount the ConfigMap (if using Option A):

```yaml
volumes:
  - name: pricing-config
    configMap:
      name: hetzner-pricing
volumeMounts:
  - name: pricing-config
    mountPath: /config
```

### Step 3: Verify Configuration

Check OpenCost logs to confirm pricing is loaded:

```bash
kubectl logs -n opencost deployment/opencost | grep -i "price info"
```

You should see entries like:
```
Found price info {InstanceID:cpx21 Region:fsn1 AssetClass:node ...}
```

## Pricing CSV Format

The CSV uses these columns:

| Column | Description |
|--------|-------------|
| `EndTimestamp` | Optional expiry (leave empty for current pricing) |
| `InstanceID` | The instance type identifier (e.g., `cpx21`) |
| `Region` | Datacenter region (e.g., `fsn1`, `nbg1`, `hel1`) |
| `AssetClass` | Type: `node`, `pv`, or `gpu` |
| `InstanceIDField` | Kubernetes label/field to match |
| `InstanceType` | Instance type (same as InstanceID for nodes) |
| `MarketPriceHourly` | Hourly price in EUR |
| `Version` | Version identifier for tracking |

## Regions

Hetzner Cloud operates in these regions:

| Region | Location |
|--------|----------|
| `fsn1` | Falkenstein, Germany |
| `nbg1` | Nuremberg, Germany |
| `hel1` | Helsinki, Finland |
| `ash` | Ashburn, Virginia, USA |
| `hil` | Hillsboro, Oregon, USA |
| `sin` | Singapore |

> **Note**: The provided CSV includes EU regions. Add US/Singapore entries with adjusted pricing if needed.

## Updating Prices

Hetzner occasionally updates pricing. To update:

1. Check current prices at [hetzner.com/cloud](https://www.hetzner.com/cloud/)
2. Update the CSV file
3. Recreate the ConfigMap or re-upload to S3
4. Restart OpenCost to reload pricing

## Troubleshooting

### Nodes showing $0 cost

- Verify `node.kubernetes.io/instance-type` label exists on nodes
- Check that the instance type in the label matches an entry in the CSV
- Ensure `USE_CSV_PROVIDER=true` is set

### Region mismatch

- The CSV uses region-specific pricing
- Ensure `topology.kubernetes.io/region` label matches CSV region values

### CSV not loading

- Check file path/permissions
- For S3: verify credentials and endpoint configuration
- Check OpenCost logs for error messages

## Example: Viewing Costs

Once configured, query costs via the OpenCost API:

```bash
# Get allocation by namespace
curl http://opencost.opencost:9090/allocation/compute \
  -G -d window=1d -d aggregate=namespace

# Get node costs
curl http://opencost.opencost:9090/allocation/compute \
  -G -d window=1d -d aggregate=node
```

## Related Resources

- [OpenCost Documentation](https://www.opencost.io/docs/)
- [Hetzner Cloud Pricing](https://www.hetzner.com/cloud/)
- [Hetzner Cloud API - Pricing](https://docs.hetzner.cloud/reference/cloud#pricing)
- [GitHub Issue #1974](https://github.com/opencost/opencost/issues/1974)
