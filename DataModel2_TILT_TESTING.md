# Testing DM2 POC with Tilt

This guide explains how to test the DM2 (Data Model 2.0) POC implementation using Tilt for local Kubernetes development.

## Files Created for Testing

1. **Tiltfile.dm2** - Main Tiltfile that uses DM2-specific configuration
2. **Tiltfile.opencost.dm2** - Modified OpenCost Tiltfile that builds with `dm2emitter` tag
3. **tilt-values-dm2.yaml** - Helm values with DM2 environment variables
4. **test-dm2-quick.sh** - Quick testing script

## Quick Start

### Quick Testing

After starting Tilt, run the quick test script to verify DM2 is working:

```bash
# First, start Tilt with DM2
tilt up -f Tiltfile.dm2

# In another terminal, run the test
./test-dm2-quick.sh
```

The script will:
- Check if the pod is running with DM2 enabled
- Verify environment variables are set
- Look for DM2 initialization in logs
- Check for generated files and decode them

## What the DM2 Setup Does

### Build Changes
- Compiles OpenCost with `-tags dm2emitter` flag
- Includes the DM2 emitter code in the binary
- Adds `internal/dm2emitter` to the watched dependencies

### Runtime Configuration
The Helm values configure these environment variables:
- `OPENCOST_DM2_EMITTER=on` - Enables the DM2 emitter
- `OPENCOST_DM2_OUTPUT=/tmp/dm2-output` - Output directory for DM2 files
- `OPENCOST_DM2_PERIOD=30s` - Emission interval (30 seconds for testing)
- `OPENCOST_CLUSTER_UID=tilt-cluster-dm2-poc` - Cluster identifier

### Output
- DM2 emitter creates compressed protobuf files every 30 seconds
- Files are named: `dm2_<timestamp>.pb.gz`
- Each file contains a complete snapshot of the cluster hierarchy

## Verification Steps

### 1. Check Build Tag Inclusion
```bash
# In the Tilt logs or build output, verify the build command includes the tag:
# CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -tags dm2emitter ...
```

### 2. Verify Environment Variables
```bash
POD=$(kubectl get pods -l app.kubernetes.io/name=opencost -o jsonpath='{.items[0].metadata.name}')
kubectl exec $POD -- env | grep DM2
```

Expected output:
```
OPENCOST_DM2_EMITTER=on
OPENCOST_DM2_OUTPUT=/tmp/dm2-output
OPENCOST_DM2_PERIOD=30s
OPENCOST_CLUSTER_UID=tilt-cluster-dm2-poc
```

### 3. Monitor File Generation
```bash
# Watch for new files being created
kubectl exec $POD -- sh -c 'while true; do ls -la /tmp/dm2-output/ 2>/dev/null | tail -5; sleep 10; done'
```

### 4. Analyze Output
After files are generated, use the decoder to verify the content:
```bash
./dm2decode /tmp/dm2_*.pb.gz
```

Expected output format:
```
File: /tmp/dm2_1234567890.pb.gz
Cluster: tilt-cluster-dm2-poc
Namespaces: X
Workloads: Y
Pods: Z
Containers: W
```

## Troubleshooting

### DM2 Emitter Not Starting
1. Check if binary was built with tag:
   ```bash
   kubectl exec $POD -- /app/main --version 2>&1 | grep dm2 || echo "DM2 not included in build"
   ```

2. Check initialization logs:
   ```bash
   kubectl logs $POD | grep -A5 -B5 "DM2"
   ```

### No Output Files
1. Verify environment variables are set
2. Check if output directory exists:
   ```bash
   kubectl exec $POD -- ls -la /tmp/ | grep dm2
   ```
3. Wait at least 30 seconds (configured emission period)
4. Check for permission issues in logs

### Files Not Decodable
1. Ensure decoder is built with tag:
   ```bash
   go build -tags dm2emitter -o dm2decode ./cmd/dm2decode
   ```
2. Verify file is not corrupted during copy
3. Check file size (should be > 0 bytes)

## Cleanup

To stop the test deployment:
```bash
# If using Tilt UI
# Press Ctrl+C in the terminal where tilt is running

# Or use Tilt down
tilt down -f Tiltfile.dm2

# Clean up test files
rm -f /tmp/dm2_*.pb.gz
rm -f dm2decode opencost-dm2-test
```

## Normal Tilt Usage (Without DM2)

To run the normal Tilt setup without DM2:
```bash
tilt up  # Uses the regular Tiltfile
```

This will build and deploy OpenCost without the DM2 emitter code.

## Summary

The DM2 POC integration with Tilt provides:
1. **Safe testing** - DM2 code only included when explicitly using Tiltfile.dm2
2. **Easy verification** - Script automates common testing tasks
3. **Live updates** - Tilt's hot reload works with DM2 code changes
4. **Complete isolation** - Normal Tilt workflow unaffected

The POC successfully demonstrates that the DM2 emitter can be integrated into OpenCost's development workflow without disrupting existing processes.