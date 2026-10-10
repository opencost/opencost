package costmodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util"
	"github.com/opencost/opencost/pkg/cloud/models"
)

type quotaMockProvider struct {
	models.Provider
}

func (mp *quotaMockProvider) GetConfig() (*models.CustomPricing, error) {
	return &models.CustomPricing{
		CustomPricesEnabled: "false",
		CPU:                 "0.05",
		RAM:                 "0.01",
	}, nil
}

func (mp *quotaMockProvider) CombinedDiscountForNode(nodeName string, isPreemptible bool, defaultCPUDiscount float64, defaultRAMDiscount float64) float64 {
	return 0
}

func TestQuotaOverheadCalculation(t *testing.T) {
	t.Setenv("CLUSTER_ID", "test-cluster")

	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)

	ds := source.NewMockOpenCostDataSource()
	ds.ResolutionValue = 5 * time.Minute

	// Seed cluster info and uptime
	ds.Querier.SetOverride(source.QueryClusterInfo, []*source.ClusterInfoResult{
		{UID: "test-cluster", Cluster: "my-cluster", Provider: "aws"},
	})
	ds.Querier.SetOverride(source.QueryClusterUptime, []*source.UptimeResult{
		{UID: "test-cluster", First: start, Last: end},
	})

	// Seed namespace info and uptime
	ds.Querier.SetOverride(source.QueryNamespaceInfo, []*source.NamespaceInfoResult{
		{UID: "ns-1", Namespace: "tenant-a"},
	})
	ds.Querier.SetOverride(source.QueryNamespaceUptime, []*source.UptimeResult{
		{UID: "ns-1", First: start, Last: end},
	})

	// Seed resource quota info, uptime, and spec limits (requests CPU = 10 cores, requests RAM = 32 GiB)
	ds.Querier.SetOverride(source.QueryResourceQuotaInfo, []*source.ResourceQuotaInfoResult{
		{UID: "rq-1", ResourceQuota: "quota-a", NamespaceUID: "ns-1"},
	})
	ds.Querier.SetOverride(source.QueryResourceQuotaUptime, []*source.UptimeResult{
		{UID: "rq-1", First: start, Last: end},
	})
	// cpu hard = 10 cores
	ds.Querier.SetOverride(source.QueryResourceQuotaSpecCPURequestAverage, []*source.ResourceResult{
		{UID: "rq-1", Value: 10.0},
	})
	// memory hard = 32 GiB (34359738368 bytes)
	ds.Querier.SetOverride(source.QueryResourceQuotaSpecRAMRequestAverage, []*source.ResourceResult{
		{UID: "rq-1", Value: 32 * 1024 * 1024 * 1024},
	})

	// Seed QueryPods to initialize podMap
	ds.Querier.SetOverride(source.QueryPods, []*source.PodsResult{
		{
			Cluster:   "test-cluster",
			Namespace: "tenant-a",
			Pod:       "pod-a",
			Data: []*util.Vector{
				{Value: 60.0, Timestamp: float64(start.Unix())},
			},
		},
	})

	// Seed pod request metrics (CPU cores allocated = 2, requests average = 2, RAM bytes = 8 GiB)
	ds.Querier.SetOverride(source.QueryCPUCoresAllocated, []*source.ContainerMetricResult{
		{
			Cluster: "test-cluster",
			Data: []*util.Vector{
				{Value: 2.0, Timestamp: float64(start.Unix())},
			},
			Pod:       "pod-a",
			Namespace: "tenant-a",
			Container: "container-a",
			Node:      "node-1",
		},
	})
	ds.Querier.SetOverride(source.QueryCPURequests, []*source.ContainerMetricResult{
		{
			Cluster: "test-cluster",
			Data: []*util.Vector{
				{Value: 2.0, Timestamp: float64(start.Unix())},
			},
			Pod:       "pod-a",
			Namespace: "tenant-a",
			Container: "container-a",
			Node:      "node-1",
		},
	})
	ds.Querier.SetOverride(source.QueryCPUUsageAvg, []*source.ContainerMetricResult{
		{
			Cluster: "test-cluster",
			Data: []*util.Vector{
				{Value: 1.0, Timestamp: float64(start.Unix())},
			},
			Pod:       "pod-a",
			Namespace: "tenant-a",
			Container: "container-a",
			Node:      "node-1",
		},
	})
	ds.Querier.SetOverride(source.QueryRAMBytesAllocated, []*source.ContainerMetricResult{
		{
			Cluster: "test-cluster",
			Data: []*util.Vector{
				{Value: 8 * 1024 * 1024 * 1024, Timestamp: float64(start.Unix())},
			},
			Pod:       "pod-a",
			Namespace: "tenant-a",
			Container: "container-a",
			Node:      "node-1",
		},
	})
	ds.Querier.SetOverride(source.QueryRAMRequests, []*source.ContainerMetricResult{
		{
			Cluster: "test-cluster",
			Data: []*util.Vector{
				{Value: 8 * 1024 * 1024 * 1024, Timestamp: float64(start.Unix())},
			},
			Pod:       "pod-a",
			Namespace: "tenant-a",
			Container: "container-a",
			Node:      "node-1",
		},
	})
	ds.Querier.SetOverride(source.QueryRAMUsageAvg, []*source.ContainerMetricResult{
		{
			Cluster: "test-cluster",
			Data: []*util.Vector{
				{Value: 4 * 1024 * 1024 * 1024, Timestamp: float64(start.Unix())},
			},
			Pod:       "pod-a",
			Namespace: "tenant-a",
			Container: "container-a",
			Node:      "node-1",
		},
	})

	// Seed node pricing (CPU = $0.05 / hour, RAM = $0.01 / GiB hour)
	ds.Querier.SetOverride(source.QueryNodeCPUPricePerHr, []*source.NodeCPUPricePerHrResult{
		{
			Cluster: "test-cluster",
			Node:    "node-1",
			Data: []*util.Vector{
				{Value: 0.05, Timestamp: float64(start.Unix())},
			},
		},
	})
	ds.Querier.SetOverride(source.QueryNodeRAMPricePerGiBHr, []*source.NodeRAMPricePerGiBHrResult{
		{
			Cluster: "test-cluster",
			Node:    "node-1",
			Data: []*util.Vector{
				{Value: 0.01, Timestamp: float64(start.Unix())},
			},
		},
	})

	// Instantiate CostModel
	cm := NewCostModel("test-cluster", ds, &quotaMockProvider{}, nil, nil, time.Hour)
	require.NotNil(t, cm)

	kms, err := cm.ComputeKubeModelSet(start, end)
	t.Logf("KMS error: %v", err)
	if kms != nil {
		t.Logf("KMS ResourceQuotas: %+v", kms.ResourceQuotas)
		t.Logf("KMS Namespaces: %+v", kms.Namespaces)
	}

	// Compute allocations
	allocSet, err := cm.ComputeAllocation(start, end)
	require.NoError(t, err)
	require.NotNil(t, allocSet)

	// Check that we have the quota overhead allocation
	overheadName := "test-cluster/__quota_overhead__/tenant-a/__quota_overhead__/__quota_overhead__"
	overheadAlloc, ok := allocSet.Allocations[overheadName]
	if !ok {
		for k := range allocSet.Allocations {
			t.Logf("Found allocation key: %s", k)
		}
	}
	require.True(t, ok, "expected to find quota overhead allocation")

	// Verify math:
	// Unused CPU quota = 10 - 2 = 8 cores
	// CPU unit price = $0.05 / hour
	// Hours = 1
	// expected CPU overhead cost = 8 * 0.05 * 1 = $0.40
	// Unused RAM quota = 32 - 8 = 24 GiB
	// RAM unit price = $0.01 / hour
	// expected RAM overhead cost = 24 * 0.01 * 1 = $0.24
	// Total expected overhead cost = $0.64
	assert.InDelta(t, 0.40, overheadAlloc.QuotaOverheadCPUCost, 1e-9)
	assert.InDelta(t, 0.24, overheadAlloc.QuotaOverheadRAMCost, 1e-9)
	assert.InDelta(t, 0.64, overheadAlloc.QuotaOverheadCost, 1e-9)
	assert.InDelta(t, 0.64, overheadAlloc.TotalCost(), 1e-9)
}
