# OpenCost Data Model 2.0 (DM2) Emitter - Complete Documentation

## Overview

Successfully implemented a **safe, optional** Data Model 2.0 (DM2) emitter that exports a **UID-first** hierarchical view of Kubernetes objects as **compressed protobuf**. The POC is fully isolated and does not affect normal builds or runtime unless explicitly enabled.

The DM2 emitter creates a hierarchical representation that preserves Kubernetes UIDs throughout:
```
Cluster > Namespace > Workload > Pod > Container
```

Each level uses UIDs as primary identifiers, enabling stable references across the Kubernetes ecosystem.

## Implementation Status: ✅ COMPLETE

### Components Implemented

#### 1. Protobuf Schema 
- **File**: `protos/dm2/opencost_dm2.proto`
- **Generated Types**: `protos/dm2/opencost_dm2.pb.go`
- Defines UID-first hierarchy: `Cluster > Namespace > Workload > Pod > Container`

#### 2. Build Infrastructure 
- **Generation Script**: `tools/gen-protos.sh`
- Automated protobuf code generation

#### 3. Emitter Package 
- **Location**: `internal/dm2emitter/`
- **Files**:
  - `emitter.go`: Core emitter logic with build tag `//go:build dm2emitter`
  - `adapter.go`: Real Kubernetes cache adapter implementation
  - `emitter_test.go`: Unit tests
  - `README_POC.md`: Documentation
  - `INTEGRATION_NOTES.md`: Integration notes

#### 4. Integration Hooks 
- **Package-level hooks**: 
  - `pkg/costmodel/dm2_init_enabled.go` (with tag)
  - `pkg/costmodel/dm2_init_disabled.go` (without tag)
- **Integration**: Called from `pkg/costmodel/router.go:574`

#### 5. Verification Tools 
- **Decode Tool**: `cmd/dm2decode/main.go`
- Reads and displays counts from `.pb.gz` files

## Architecture & Design

### Features

- **UID-first design**: All objects are identified primarily by their UIDs
- **Compressed protobuf format**: Efficient binary serialization with gzip compression
- **Build-time isolation**: Only compiled with `-tags dm2emitter`
- **Runtime isolation**: Only runs when `OPENCOST_DM2_EMITTER=on`
- **Zero impact on normal builds**: No code is included in standard builds

### Schema Details

The protobuf schema is defined in `protos/dm2/opencost_dm2.proto`:

- **Cluster**: Root object with cluster UID, name, and namespaces
- **Namespace**: Contains UID, name, and workloads
- **Workload**: Represents controllers (Deployment, StatefulSet, etc.) with pods
- **Pod**: Contains UID, name, node assignment, and containers
- **Container**: Includes UID (derived), name, and image

### Output Format

The emitter produces gzipped protobuf files named with Unix timestamps:
```
dm2_1234567890.pb.gz
```

Each file contains a complete snapshot of the cluster hierarchy at that point in time.

## Adapter Implementation Details

The adapter (`adapter.go`) successfully bridges OpenCost's clustercache with the DM2 data model:

- **Namespaces**: Uses actual Kubernetes UIDs from `clustercache.Namespace.UID`
- **Workloads**: Derived from Pod owner references
- **Pods**: Direct mapping with UID preservation
- **Containers**: Derives stable UIDs as `podUID/containerName`, gets image from `ContainerStatus`
- **Nodes**: Uses actual Kubernetes UIDs from `clustercache.Node.UID`

## Build and Runtime Guards

### Compile-time Guard
- Files with `//go:build dm2emitter` are compiled **only** with `-tags dm2emitter`
- Normal builds (`go build ./...`) exclude all DM2 code

### Runtime Guard
- Even when compiled with the tag, emitter only runs if `OPENCOST_DM2_EMITTER=on`
- Environment variables:
  - `OPENCOST_DM2_EMITTER=on`: Enable emitter
  - `OPENCOST_DM2_OUTPUT=/path`: Output directory (default: `/tmp`)
  - `OPENCOST_DM2_PERIOD=5m`: Emission interval (default: `5m`)
  - `OPENCOST_CLUSTER_UID`: Optional cluster UID override

## How to Test

### Recommended: Testing with Tilt

For development and testing, use the Tilt setup which provides hot-reload and easy verification:

```bash
# Start Tilt with DM2 enabled
tilt up -f Tiltfile.dm2

# In another terminal, verify DM2 is working
./test-dm2-quick.sh
```

See [DM2_TILT_TESTING.md](DataModel2_TILT_TESTING.md) for detailed instructions.

### Alternative: Manual Build

If you need to build manually outside of Tilt:

```bash
# Build with DM2 support
go build -tags dm2emitter -o opencost-dm2 ./cmd/costmodel

# Run with environment variables
OPENCOST_DM2_EMITTER=on \
OPENCOST_DM2_OUTPUT=/tmp \
OPENCOST_DM2_PERIOD=30s \
./opencost-dm2
```

## Integration Details

### How DM2 Emitter is Integrated

The DM2 emitter is integrated into OpenCost through a clean hook pattern:

1. **Integration Point**: The `initDM2Emitter` function is called in `pkg/costmodel/router.go:574` after the Kubernetes caches and cluster info are initialized.

2. **Hook Implementation**: 
   - `pkg/costmodel/dm2_init_enabled.go` - Contains the actual initialization logic (compiled with `-tags dm2emitter`)
   - `pkg/costmodel/dm2_init_disabled.go` - No-op stub (compiled without the tag)

3. **Dependencies Used**:
   - `ClusterCache`: Source of Kubernetes object data
   - `ClusterInfoProvider`: Source of cluster metadata

This design ensures zero impact on normal builds while allowing the DM2 emitter to access all necessary OpenCost infrastructure when enabled.

## Testing

### Unit Tests
```bash
go test -tags dm2emitter ./internal/dm2emitter/...
# ok  github.com/opencost/opencost/internal/dm2emitter  0.006s
```

### Build Tests
```bash
# Normal build (without tag) - PASSED
go build ./cmd/costmodel
# No DM2 symbols in binary

# Tagged build - PASSED
go build -tags dm2emitter ./cmd/costmodel
# DM2 code included but inactive without env var
```

### Integration Testing

1. Build without the tag and verify no DM2 code is included:
   ```bash
   go build ./cmd/costmodel
   ./costmodel  # Should run normally without DM2
   ```

2. Build with the tag but without the env var:
   ```bash
   go build -tags dm2emitter ./cmd/costmodel
   ./costmodel  # Should log that DM2 is compiled but not enabled
   ```

3. Build with the tag and enable via env:
   ```bash
   go build -tags dm2emitter ./cmd/costmodel
   OPENCOST_DM2_EMITTER=on ./costmodel  # Should start emitting DM2 files
   ```

### Decode Tool Testing
```bash
go build -tags dm2emitter -o /tmp/dm2decode ./cmd/dm2decode
# Build successful
```

## Safety Guarantees

1. **Zero impact on normal builds**: DM2 code is completely excluded without build tag
2. **Zero impact on tagged builds without env var**: Code is present but dormant
3. **Isolated implementation**: All DM2 code is in separate files with build tags
4. **No new dependencies**: Only uses existing OpenCost dependencies and `google.golang.org/protobuf`

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

## Next Steps & Future Work

This POC provides a solid foundation for the DM2 emitter. Future enhancements could include:

1. **Parity Testing**: Compare DM2 output with existing metrics for validation
2. **Additional Metadata**: 
   - Add metrics/cost data to the hierarchy
   - Include resource requests/limits
   - Add labels and annotations as needed
3. **Performance Optimization**: Batch operations, caching for large clusters
4. **Export Options**: Support for different output formats/destinations
5. **Streaming Updates**: Implement streaming updates instead of snapshots
6. **Monitoring**: Add metrics for emitter health and performance
7. **Comparison Tools**: Add tools for validating against current data model

## Conclusion

The DM2 emitter POC is **fully functional** and ready for testing. It successfully:
- Preserves Kubernetes UIDs throughout the hierarchy
- Extracts container images from pod status
- Maintains complete isolation from normal builds
- Provides compressed protobuf output as specified
- Includes decode tool for verification

This implementation serves as a robust starting point for further development and integration into OpenCost's data model evolution.

## Notes

This is a proof-of-concept implementation. The schema and implementation will evolve based on feedback and requirements from the OpenCost community.