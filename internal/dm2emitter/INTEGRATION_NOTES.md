# DM2 Emitter Integration Notes

## Where to Call startDM2Emitter

The `startDM2Emitter` hook should be called after the Kubernetes caches and cluster info are initialized.

### Option 1: In pkg/cmd/costmodel/costmodel.go (Recommended)

After line 37 where `costmodel.Initialize(router)` is called:

```go
if conf.KubernetesEnabled {
    a = costmodel.Initialize(router)
    
    // Start DM2 emitter if compiled in (no-op if not)
    // The hook is defined in cmd/costmodel/dm2_hook_enabled.go or dm2_hook_disabled.go
    // Pass the cache and cluster info from the Accesses struct
    startDM2Emitter(context.Background(), a.Cache, a.ClusterInfo)
    
    err := StartExportWorker(context.Background(), a.Model)
    // ...
}
```

### Option 2: In pkg/costmodel/router.go Initialize function

At the end of the `Initialize` function (around line 600), after all components are set up:

```go
func Initialize(router *httprouter.Router, additionalConfigWatchers ...*watcher.ConfigMapWatcher) *Accesses {
    // ... existing initialization code ...
    
    // At the very end, before returning:
    // Start DM2 emitter if available (requires importing cmd/costmodel package)
    // This would need careful handling to avoid circular dependencies
    
    return &Accesses{
        // ... existing fields ...
    }
}
```

### Recommended Approach

Option 1 is cleaner because:
1. It keeps the integration at the application level (cmd/costmodel)
2. No risk of circular dependencies
3. The hook pattern with build tags is already set up
4. Easy to see and understand when reviewing the code

## Required Changes

To complete the integration, add this line to `pkg/cmd/costmodel/costmodel.go` after line 37:

```go
// Start DM2 emitter (no-op if not compiled with -tags dm2emitter)
startDM2Emitter(context.Background(), a.Cache, a.ClusterInfo)
```

The function signature in the hooks needs to match what's available in the Accesses struct. You may need to adjust the parameters based on the actual types available.

## Testing the Integration

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

## Accessing Required Dependencies

The `Accesses` struct returned by `costmodel.Initialize` should contain:
- `Cache`: The cluster cache for Kubernetes objects
- `ClusterInfo`: The cluster information provider

These are exactly what the DM2 emitter needs to operate.