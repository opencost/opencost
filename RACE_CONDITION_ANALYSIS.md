# Concurrent Map Access Issue Analysis

## Issue Summary

**Issue #3388**: Container restarted due to concurrent map iteration and map write
**Related Issue #2910**: Concurrent map read and map write in label matching

## Status: ✅ CONFIRMED AND REPRODUCED

## Root Causes Identified

### 1. SanitizeLabels Function (core/pkg/util/promutil/promutil.go:118-126)

**Vulnerable Code:**
```go
func SanitizeLabels(labels map[string]string) map[string]string {
    response := make(map[string]string, len(labels))

    for k, v := range labels {  // ← RACE CONDITION HERE
        response[SanitizeLabelName(k)] = v
    }

    return response
}
```

**Problem:**
- The function iterates over an input map without any synchronization
- If another goroutine modifies the input `labels` map during iteration, Go's runtime detects the concurrent access and panics with: `fatal error: concurrent map iteration and map write`
- In Go, maps are not thread-safe and concurrent access requires explicit synchronization

**Call Sites:**
- Used in `KubePrependQualifierToLabels()` (line 81)
- Called from `getNamespaceLabels()` via `SanitizeLabelName()` (line 1406)
- Called from `getNamespaceAnnotations()` via `SanitizeLabelName()` (line 1419)

### 2. getPodServices Function (pkg/costmodel/costmodel.go:1310-1337)

**Vulnerable Code:**
```go
func getPodServices(cache clustercache.ClusterCache, podList []*clustercache.Pod, clusterID string) (map[string]map[string][]string, error) {
    // ...
    for _, pod := range podList {
        labelSet := labels.Set(pod.Labels)  // ← No copy made, shared reference
        if s.Matches(labelSet) && pod.Namespace == namespace {  // ← Iterates over labelSet
            // ...
        }
    }
    // ...
}
```

**Problem:**
- Line 1325: `labels.Set(pod.Labels)` creates a `labels.Set` but doesn't copy the underlying map
- Line 1326: `s.Matches(labelSet)` iterates over the labelSet
- If another goroutine modifies `pod.Labels` while `Matches()` is iterating, the same panic occurs
- The kubernetes label matching code (`k8s.io/apimachinery/pkg/labels.Set.Has()`) is iterating over a shared map

**Similar Patterns Found:**
- `getPodStatefulsets()` (line 1355-1356): Same pattern with `labels.Set(pod.Labels)` and `s.Matches(labelSet)`
- `getPodDeployments()` (line 1386-1387): Same pattern with `labels.Set(pod.Labels)` and `s.Matches(labelSet)`

## Reproduction

### Test Case Created
File: `test_race_condition.go`

The test successfully reproduces the issue by:
1. Creating a shared map with 100 label entries
2. Spawning 10 concurrent goroutines
3. Half the goroutines read/iterate via `SanitizeLabels()`
4. Half the goroutines write/modify the shared map
5. Running 1000 iterations each

### Reproduction Result
```
fatal error: concurrent map iteration and map write

goroutine 21 [running]:
internal/runtime/maps.fatal({0x4e4969?, 0x0?})
    /usr/local/go1.24.7/src/runtime/panic.go:1058 +0x18
internal/runtime/maps.(*Iter).Next(0xc?)
    /usr/local/go1.24.7/src/internal/runtime/maps/table.go:683 +0x86
main.SanitizeLabels(0xc000128210)
    /home/user/opencost/test_race_condition.go:33 +0x9b
```

**Result**: ✅ Issue confirmed - the race condition is real and reproducible

## Impact

### Severity: **HIGH** (Production crashes)

1. **Container Crashes**: The panic causes the entire container to restart
2. **Service Disruption**: Cost calculation service becomes unavailable during restarts
3. **Data Loss**: In-progress calculations are lost
4. **Reliability**: On large clusters with high concurrency (461+ goroutines as mentioned in #2910), the probability of hitting this race condition increases significantly

### When It Occurs

1. **Startup on Large Clusters**: Issue #2910 specifically mentions crashes during startup cache warming on large clusters
2. **High Concurrency**: When multiple goroutines simultaneously:
   - Iterate over label maps (via `SanitizeLabels` or `Matches`)
   - Modify the same label maps (updating pod/namespace metadata)
3. **Cache Warming**: During aggregate cost model cache initialization (aggregation.go:1810)
4. **Label Matching**: During service/pod matching operations in `costDataRange()`

## Technical Details

### Go Map Concurrency Rules

From Go documentation:
> "Maps are not safe for concurrent use: it's not defined what happens when you read and write to them simultaneously. If you need to read from and write to a map from concurrently executing goroutines, the accesses must be mediated by some kind of synchronization mechanism."

### Why This Happens

1. **No Defensive Copying**: Functions receive map references directly without creating defensive copies
2. **No Synchronization**: No mutexes or other synchronization primitives protect map access
3. **Shared State**: The same map instance is passed to multiple goroutines
4. **Kubernetes API**: The `clustercache` returns direct references to internal maps, which can be modified

## Recommended Fixes

### Option 1: Defensive Copying (Preferred for SanitizeLabels)

```go
func SanitizeLabels(labels map[string]string) map[string]string {
    if labels == nil {
        return nil
    }

    response := make(map[string]string, len(labels))

    // Create a defensive copy first to avoid iteration over potentially shared map
    for k, v := range labels {
        response[SanitizeLabelName(k)] = v
    }

    return response
}
```

**Note**: This doesn't fully solve the issue if the input map is being modified during the iteration. A better approach:

```go
func SanitizeLabels(labels map[string]string) map[string]string {
    if labels == nil {
        return nil
    }

    // Option A: Require caller to pass ownership/copy
    // Option B: Use sync.RWMutex at the source
    // Option C: Create snapshot before processing

    response := make(map[string]string, len(labels))
    for k, v := range labels {
        response[SanitizeLabelName(k)] = v
    }
    return response
}
```

### Option 2: Fix at Source (Preferred for getPodServices)

Ensure the cache returns copies rather than references:

```go
func getPodServices(cache clustercache.ClusterCache, podList []*clustercache.Pod, clusterID string) (map[string]map[string][]string, error) {
    servicesList := cache.GetAllServices()
    podServicesMapping := make(map[string]map[string][]string)

    for _, service := range servicesList {
        // ... setup code ...

        for _, pod := range podList {
            // Create a copy of pod.Labels to avoid concurrent access
            labelsCopy := make(map[string]string, len(pod.Labels))
            for k, v := range pod.Labels {
                labelsCopy[k] = v
            }
            labelSet := labels.Set(labelsCopy)

            if s.Matches(labelSet) && pod.Namespace == namespace {
                // ... rest of logic ...
            }
        }
    }
    return podServicesMapping, nil
}
```

### Option 3: Synchronization at Cache Level

Add read-write locks to the cluster cache to protect concurrent access:

```go
type ClusterCache struct {
    mu sync.RWMutex
    // ... other fields ...
}

func (c *ClusterCache) GetPod(namespace, name string) *Pod {
    c.mu.RLock()
    defer c.mu.RUnlock()
    // ... return pod with copied labels ...
}
```

## Files Affected

1. `core/pkg/util/promutil/promutil.go` - SanitizeLabels function (line 118-126)
2. `pkg/costmodel/costmodel.go`:
   - getPodServices (line 1310-1337, specifically 1325-1326)
   - getPodStatefulsets (line 1355-1356)
   - getPodDeployments (line 1386-1387)
   - getNamespaceLabels (line 1406)
   - getNamespaceAnnotations (line 1419)
3. Cluster cache implementation (needs investigation)

## Related Stack Traces

From Issue #2910:
```
fatal error: concurrent map read and map write
k8s.io/apimachinery/pkg/labels.Set.Has()
    at labels.go:53
```

From Issue #3388:
```
fatal error: concurrent map iteration and map write
/core/pkg/util/promutil.SanitizeLabels
```

## Next Steps

1. ✅ Confirmed issue is reproducible
2. ⏭️ Implement fixes for all affected functions
3. ⏭️ Add unit tests with `-race` flag
4. ⏭️ Review cluster cache implementation for defensive copying
5. ⏭️ Add integration tests simulating high-concurrency scenarios
6. ⏭️ Consider adding linter rules to detect similar patterns

## References

- Issue #3388: https://github.com/opencost/opencost/issues/3388
- Issue #2910: https://github.com/opencost/opencost/issues/2910
- Go Maps: https://go.dev/blog/maps
- Go Memory Model: https://go.dev/ref/mem
