package costmodel

import (
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util"
	"github.com/opencost/opencost/pkg/cloud/provider"
	"github.com/opencost/opencost/pkg/clustercache"
	"github.com/opencost/opencost/pkg/config"
	"github.com/opencost/opencost/pkg/env"
	"github.com/opencost/opencost/pkg/prom"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
)

type FakeClusterMap struct {
	clusters.ClusterMap
}

type FakeClusterCache struct {
	clustercache.ClusterCache
}

func (f FakeClusterCache) GetAllNodes() []*v1.Node {
	return nil
}

func (f FakeClusterCache) GetAllDaemonSets() []*appsv1.DaemonSet {
	return nil
}

func NewTestCostModel(t *testing.T) *CostModel {
	t.Helper()
	confManager := config.NewConfigFileManager(&config.ConfigFileManagerOpts{
		BucketStoreConfig: env.GetKubecostConfigBucket(),
		LocalConfigPath:   "../../",
	})

	clusterCache := FakeClusterCache{}
	cloudProvider, err := provider.NewProvider(clusterCache, "", confManager)
	require.NoError(t, err)
	return NewCostModel(nil, cloudProvider, clusterCache, FakeClusterMap{}, time.Minute)
}

func TestCostModel_calcAllocation(t *testing.T) {
	t.Run("empty data", func(t *testing.T) {
		end := time.Now()
		start := end.Add(-time.Duration(24) * time.Hour)

		costModel := NewTestCostModel(t)

		res, _, err := costModel.calcAllocation(start, end, time.Hour, &allocationPromData{}, false)
		require.NoError(t, err)
		require.Equal(t, map[string]*opencost.Allocation{}, res.Allocations)
	})

	t.Run("simple case", func(t *testing.T) {
		end := time.Now()
		start := end.Add(-24 * time.Hour)

		costModel := NewTestCostModel(t)

		wStart := start
		wEnd := wStart.Add(15 * time.Minute)

		res, _, err := costModel.calcAllocation(start, end, time.Hour, &allocationPromData{
			RAMBytesAllocated: []*prom.QueryResult{
				{
					Metric: map[string]interface{}{
						"container": "autoscaler",
						"namespace": "kube-system",
						"node":      "node1",
						"pod":       "autoscaler-1",
					},
					Values: []*util.Vector{
						{
							Timestamp: float64(end.Unix()),
							Value:     100_000_000,
						},
					},
				},
			},
			PodMap: map[podKey]*pod{
				podKey{
					namespaceKey: namespaceKey{
						Namespace: "kube-system",
					},
					Pod: "autoscaler-1",
				}: {
					Allocations: map[string]*opencost.Allocation{},
					Key: podKey{
						namespaceKey: namespaceKey{
							Namespace: "kube-system",
						},
						Pod: "autoscaler-1",
					},
					Start: wStart,
					End:   wEnd,
				},
			},
		}, true)
		require.NoError(t, err)
		require.Equal(t, map[string]*opencost.Allocation{
			"/node1/kube-system/autoscaler-1/autoscaler": {
				Name:  "/node1/kube-system/autoscaler-1/autoscaler",
				Start: wStart,
				End:   wEnd,
				Properties: &opencost.AllocationProperties{
					Node:                 "node1",
					Container:            "autoscaler",
					Namespace:            "kube-system",
					Pod:                  "autoscaler-1",
					Labels:               map[string]string{},
					Annotations:          map[string]string{},
					NamespaceAnnotations: map[string]string{},
					NamespaceLabels:      map[string]string{},
				},
				RAMByteHours: 25_000_000,
				RAMCost:      0.00006805639714002609,
			},
		}, res.Allocations)
	})
}
