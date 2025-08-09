# DM2 Emitter (POC)

The DM2 (Data Model 2.0) emitter exports a UID-first hierarchical view of Kubernetes objects as compressed protobuf files. This is a proof-of-concept implementation that demonstrates an alternative data model for OpenCost.

## Architecture

The emitter creates a hierarchical representation:
```
Cluster > Namespace > Workload > Pod > Container
```

Each level uses UIDs as primary identifiers, enabling stable references across the Kubernetes ecosystem.

## Features

- **UID-first design**: All objects are identified primarily by their UIDs
- **Compressed protobuf format**: Efficient binary serialization with gzip compression
- **Build-time isolation**: Only compiled with `-tags dm2emitter`
- **Runtime isolation**: Only runs when `OPENCOST_DM2_EMITTER=on`
- **Zero impact on normal builds**: No code is included in standard builds

## Building

Build with the dm2emitter tag:
```bash
go build -tags dm2emitter ./cmd/costmodel
```

Without the tag, the emitter code is completely excluded from the binary.

## Running

Set environment variables to enable and configure the emitter:

```bash
# Required to enable the emitter
export OPENCOST_DM2_EMITTER=on

# Optional: output directory (default: /tmp)
export OPENCOST_DM2_OUTPUT=/var/lib/opencost/dm2

# Optional: emission period (default: 5m)
export OPENCOST_DM2_PERIOD=1m

# Optional: cluster UID override
export OPENCOST_CLUSTER_UID=my-cluster-uid

# Run the cost model
./costmodel
```

## Output Format

The emitter produces gzipped protobuf files named with Unix timestamps:
```
dm2_1234567890.pb.gz
```

Each file contains a complete snapshot of the cluster hierarchy at that point in time.

## Schema

The protobuf schema is defined in `protos/dm2/opencost_dm2.proto`:

- **Cluster**: Root object with cluster UID, name, and namespaces
- **Namespace**: Contains UID, name, and workloads
- **Workload**: Represents controllers (Deployment, StatefulSet, etc.) with pods
- **Pod**: Contains UID, name, node assignment, and containers
- **Container**: Includes UID (derived), name, and image

## Testing

Run tests with the build tag:
```bash
go test -tags dm2emitter ./internal/dm2emitter/...
```

## Decoding Files

A simple decoder tool can be built to inspect the output files:
```bash
go build -tags dm2emitter ./cmd/dm2decode
./dm2decode /tmp/dm2_*.pb.gz
```

## Design Principles

1. **UID-first**: UIDs are the primary identifiers, names are metadata
2. **Hierarchical**: Natural Kubernetes ownership hierarchy
3. **Extensible**: Field numbers are reserved for future additions
4. **Efficient**: Binary protobuf with compression for minimal storage
5. **Safe**: Build and runtime guards ensure zero impact on production

## Integration Points

The emitter integrates with OpenCost's existing infrastructure:

- `clustercache.ClusterCache`: Source of Kubernetes object data
- `clusters.ClusterInfoProvider`: Source of cluster metadata
- Called from main application after caches are initialized

## Future Work

- Add metrics/cost data to the hierarchy
- Include resource requests/limits
- Add labels and annotations as needed
- Implement streaming updates instead of snapshots
- Add comparison tools for validating against current data model

## Notes

This is a proof-of-concept implementation. The schema and implementation will evolve based on feedback and requirements from the OpenCost community.