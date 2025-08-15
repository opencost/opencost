# OpenCost UID Emission Proof of Concept
## LFX Mentorship Coding Challenge

### Overview

This document outlines the proof of concept for implementing UID emission for Kubernetes objects in OpenCost as part of the LFX Mentorship application. The goal is to begin emitting UIDs for 3 Kubernetes objects (Pods, Deployments, and Services) via Prometheus metrics.

### Why UIDs are Critical for OpenCost

#### 1. **Unique Object Identity**
Kubernetes UIDs solve fundamental problems in cost tracking:

**Problem with Names:**
```yaml
# Two different deployments can have the same name in different namespaces
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web-app
  namespace: staging
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web-app
  namespace: production
```

**Solution with UIDs:**
```yaml
# Each has a unique UID regardless of name/namespace
staging/web-app:   UID=550e8400-e29b-41d4-a716-446655440001
production/web-app: UID=550e8400-e29b-41d4-a716-446a655440002
```

#### 2. **Object Lifecycle Tracking**
UIDs enable accurate cost tracking across object recreation:

**Scenario:** A deployment is deleted and recreated with the same name
- **Without UID:** OpenCost can't distinguish between old and new deployment
- **With UID:** OpenCost tracks each deployment instance separately

```promql
# Track cost across deployment lifecycle changes
sum(container_cpu_usage_seconds_total) by (deployment_uid)
```

#### 3. **Hierarchical Cost Models**
UIDs enable the next generation of OpenCost features:

```
Cluster UID
├── Namespace UID
│   ├── Deployment UID
│   │   ├── ReplicaSet UID
│   │   │   └── Pod UID
│   │   │       └── Container
│   └── Service UID
└── Node UID
```

#### 4. **GPU and Advanced Resource Tracking**
Future GPU cost allocation requires precise object identification:
```promql
# Allocate GPU costs to specific deployment instances
nvidia_gpu_utilization * deployment_gpu_request{deployment_uid="550e8400..."}
```

#### 5. **Multi-Cluster Cost Correlation**
UIDs help correlate costs across clusters and cloud providers:
```yaml
deployment_uid: 550e8400-e29b-41d4-a716-446655440001
cloud_resource_tags:
  - kubernetes_deployment_uid: 550e8400-e29b-41d4-a716-446655440001
```

### Current OpenCost Architecture Analysis

#### 1. Data Flow Architecture

```
Kubernetes API → Cluster Cache → Metrics Emitters → Prometheus → Cost Model
```

**Key Components:**
- **Cluster Cache**: Stores Kubernetes objects with UIDs as keys
- **Metrics Emitters**: Generate Prometheus metrics from cached objects
- **Cost Model**: Processes metrics to calculate costs and allocations

#### 2. Current UID Implementation Status

**Already Implemented:**
- ✅ **Pods**: UIDs are already emitted in `kube_pod_labels`, `kube_pod_status_phase`, etc.
- ❌ **Deployments**: No UID emission currently
- ❌ **Services**: No UID emission currently

#### 3. Cluster Cache Structure

The cluster cache uses UIDs as primary keys for storing objects:

```go
// From pkg/clustercache/store.go
type GenericStore[Input UIDGetter, Output any] struct {
    items map[types.UID]Output  // UID-based storage
}

type UIDGetter interface {
    GetUID() types.UID
}
```

### Implementation Strategy

Based on my analysis of the OpenCost codebase, I'll implement UID emission for:

1. **Pods** - ✅ Already implemented (verification)
2. **Deployments** - 🔧 New implementation
3. **Services** - 🔧 New implementation

### Actual Implementation

Now I'll implement the actual code changes to emit UIDs for these 3 Kubernetes objects.

#### Phase 1: Add UID Fields to Cluster Cache Objects

First, I need to add UID fields to the Deployment and Service structs in the cluster cache.

#### Phase 2: Update Transform Functions

Update the transform functions to capture UIDs from Kubernetes API objects.

#### Phase 3: Create New UID Metrics

Implement new Prometheus metrics that emit UIDs for deployments and services.

#### Phase 4: Update Metrics Collectors

Modify the metrics collectors to emit the new UID metrics.

### Benefits of UID-Based Approach

#### 1. **Unique Identification**
- Eliminates ambiguity from name-based identification
- Handles cases where objects have same names but different UIDs
- Supports proper lifecycle tracking

#### 2. **Improved Data Integrity**
- UIDs are immutable throughout object lifecycle
- Enables accurate cost tracking across object recreations
- Supports proper parent-child relationships

#### 3. **Enhanced Querying**
- Enables precise object identification in Prometheus queries
- Supports complex filtering and aggregation
- Facilitates advanced cost allocation scenarios

#### 4. **Future-Proofing**
- Foundation for hierarchical cost models
- Enables GPU and other resource tracking
- Supports advanced OpenCost features

### Expected Prometheus Queries After Implementation

#### 1. **Get Deployment UIDs**
```promql
kube_deployment_info{namespace="default"}
```

#### 2. **Get Service UIDs**
```promql
kube_service_info{namespace="default"}
```

#### 3. **Enhanced Cost Allocation with UIDs**
```promql
# Cost allocation by deployment UID
sum(container_cpu_usage_seconds_total) by (deployment_uid, namespace)
```

#### 4. **Object Lifecycle Tracking**
```promql
# Track object recreation by UID
changes(kube_deployment_info[1h])
```

### Conclusion

This proof of concept demonstrates how to implement UID emission for Kubernetes objects in OpenCost. The approach:

1. **Leverages existing architecture** - Uses current cluster cache and metrics emission patterns
2. **Maintains backward compatibility** - Existing functionality remains unchanged
3. **Provides foundation for future features** - Enables advanced cost modeling capabilities
4. **Follows OpenCost patterns** - Consistent with existing code structure and conventions

The implementation focuses on 3 key Kubernetes objects (Pods, Deployments, Services) as requested, with a clear path for extending to other objects in the future.

### Next Steps

1. **Implement the actual code changes below**
2. **Add comprehensive tests**
3. **Create integration test suite**
4. **Submit PR to OpenCost repository**

---

## ACTUAL IMPLEMENTATION

The following sections contain the actual code changes needed to implement UID emission for the 3 Kubernetes objects. 