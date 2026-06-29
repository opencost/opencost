package kubemodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
)

func TestComputeNodes(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)

	tests := []struct {
		name      string
		overrides map[string]any
		want      map[string]*kubemodel.Node
	}{
		{
			name:      "no data returns empty node map",
			overrides: map[string]any{},
			want:      map[string]*kubemodel.Node{},
		},
		{
			name: "basic node info and uptime",
			overrides: map[string]any{
				source.QueryNodeInfo: []*source.NodeInfoResult{
					{UID: "node-1", Node: "node-a", ProviderID: "aws:///us-east-1a/i-abc"},
					{UID: "node-2", Node: "node-b", ProviderID: "aws:///us-east-1b/i-def"},
				},
				source.QueryNodeUptime: []*source.UptimeResult{
					{UID: "node-1", First: start, Last: end},
					{UID: "node-2", First: start, Last: end},
				},
			},
			want: map[string]*kubemodel.Node{
				"node-1": {
					UID: "node-1", Name: "node-a", ProviderID: "aws:///us-east-1a/i-abc",
					Start:                start,
					End:                  end,
					ResourceCapacities:   kubemodel.ResourceQuantities{},
					ResourcesAllocatable: kubemodel.ResourceQuantities{},
				},
				"node-2": {
					UID: "node-2", Name: "node-b", ProviderID: "aws:///us-east-1b/i-def",
					Start:                start,
					End:                  end,
					ResourceCapacities:   kubemodel.ResourceQuantities{},
					ResourcesAllocatable: kubemodel.ResourceQuantities{},
				},
			},
		},
		{
			name: "node without uptime is not registered",
			overrides: map[string]any{
				source.QueryNodeInfo: []*source.NodeInfoResult{
					{UID: "node-1", Node: "node-a"},
				},
				// QueryNodeUptime intentionally absent
			},
			want: map[string]*kubemodel.Node{},
		},
		{
			name: "labels are attached to node",
			overrides: map[string]any{
				source.QueryNodeInfo: []*source.NodeInfoResult{
					{UID: "node-1", Node: "node-a"},
				},
				source.QueryNodeUptime: []*source.UptimeResult{
					{UID: "node-1", First: start, Last: end},
				},
				source.QueryNodeLabels: []*source.NodeLabelsResult{
					{UID: "node-1", Labels: map[string]string{"zone": "us-east-1a", "role": "worker"}},
				},
			},
			want: map[string]*kubemodel.Node{
				"node-1": {
					UID: "node-1", Name: "node-a",
					Start:                start,
					End:                  end,
					Labels:               map[string]string{"zone": "us-east-1a", "role": "worker"},
					ResourceCapacities:   kubemodel.ResourceQuantities{},
					ResourcesAllocatable: kubemodel.ResourceQuantities{},
				},
			},
		},
		{
			name: "resource capacities and allocatable are populated",
			overrides: map[string]any{
				source.QueryNodeInfo: []*source.NodeInfoResult{
					{UID: "node-1", Node: "node-a"},
				},
				source.QueryNodeUptime: []*source.UptimeResult{
					{UID: "node-1", First: start, Last: end},
				},
				source.QueryNodeResourceCapacities: []*source.ResourceResult{
					{UID: "node-1", Resource: "cpu", Unit: "cores", Value: 4.0},
					{UID: "node-1", Resource: "memory", Unit: "bytes", Value: 8 * 1024 * 1024 * 1024},
				},
				source.QueryNodeResourcesAllocatable: []*source.ResourceResult{
					{UID: "node-1", Resource: "cpu", Unit: "cores", Value: 3.9},
					{UID: "node-1", Resource: "memory", Unit: "bytes", Value: 7 * 1024 * 1024 * 1024},
				},
			},
			want: map[string]*kubemodel.Node{
				"node-1": {
					UID: "node-1", Name: "node-a",
					Start: start,
					End:   end,
					ResourceCapacities: kubemodel.ResourceQuantities{
						kubemodel.ResourceCPU: {
							Resource: kubemodel.ResourceCPU,
							Unit:     kubemodel.UnitCore,
							Values:   kubemodel.Stats{kubemodel.StatAvg: 4.0},
						},
						kubemodel.ResourceMemory: {
							Resource: kubemodel.ResourceMemory,
							Unit:     kubemodel.UnitByte,
							Values:   kubemodel.Stats{kubemodel.StatAvg: 8 * 1024 * 1024 * 1024},
						},
					},
					ResourcesAllocatable: kubemodel.ResourceQuantities{
						kubemodel.ResourceCPU: {
							Resource: kubemodel.ResourceCPU,
							Unit:     kubemodel.UnitCore,
							Values:   kubemodel.Stats{kubemodel.StatAvg: 3.9},
						},
						kubemodel.ResourceMemory: {
							Resource: kubemodel.ResourceMemory,
							Unit:     kubemodel.UnitByte,
							Values:   kubemodel.Stats{kubemodel.StatAvg: 7 * 1024 * 1024 * 1024},
						},
					},
				},
			},
		},
		{
			name: "local storage bytes are populated",
			overrides: map[string]any{
				source.QueryNodeInfo: []*source.NodeInfoResult{
					{UID: "node-1", Node: "node-a"},
				},
				source.QueryNodeUptime: []*source.UptimeResult{
					{UID: "node-1", First: start, Last: end},
				},
				source.QueryKMLocalStorageBytes: []*source.UIDValueResult{
					{UID: "node-1", Value: 500 * 1024 * 1024 * 1024},
				},
				source.QueryKMLocalStorageUsedAvg: []*source.NodeUIDValueResult{
					{UID: "node-1", Value: 100 * 1024 * 1024 * 1024},
				},
				source.QueryKMLocalStorageUsedMax: []*source.NodeUIDValueResult{
					{UID: "node-1", Value: 200 * 1024 * 1024 * 1024},
				},
			},
			want: map[string]*kubemodel.Node{
				"node-1": {
					UID: "node-1", Name: "node-a",
					Start: start,
					End:   end,
					FileSystem: kubemodel.FileSystem{
						CapacityBytes: 500 * 1024 * 1024 * 1024,
						UsageByteAvg:  100 * 1024 * 1024 * 1024,
						UsageByteMax:  200 * 1024 * 1024 * 1024,
					},
					ResourceCapacities:   kubemodel.ResourceQuantities{},
					ResourcesAllocatable: kubemodel.ResourceQuantities{},
				},
			},
		},
		{
			name: "uptime for unknown node is ignored",
			overrides: map[string]any{
				source.QueryNodeInfo: []*source.NodeInfoResult{
					{UID: "node-1", Node: "node-a"},
				},
				source.QueryNodeUptime: []*source.UptimeResult{
					{UID: "node-1", First: start, Last: end},
					{UID: "unknown-node", First: start, Last: end},
				},
			},
			want: map[string]*kubemodel.Node{
				"node-1": {
					UID: "node-1", Name: "node-a",
					Start:                start,
					End:                  end,
					ResourceCapacities:   kubemodel.ResourceQuantities{},
					ResourcesAllocatable: kubemodel.ResourceQuantities{},
				},
			},
		},
		{
			name: "local storage for unknown node is ignored",
			overrides: map[string]any{
				source.QueryNodeInfo: []*source.NodeInfoResult{
					{UID: "node-1", Node: "node-a"},
				},
				source.QueryNodeUptime: []*source.UptimeResult{
					{UID: "node-1", First: start, Last: end},
				},
				source.QueryKMLocalStorageBytes: []*source.UIDValueResult{
					{UID: "unknown-node", Value: 999},
				},
			},
			want: map[string]*kubemodel.Node{
				"node-1": {
					UID: "node-1", Name: "node-a",
					Start:                start,
					End:                  end,
					ResourceCapacities:   kubemodel.ResourceQuantities{},
					ResourcesAllocatable: kubemodel.ResourceQuantities{},
				},
			},
		},
		{
			name: "resource capacities for unknown node are ignored",
			overrides: map[string]any{
				source.QueryNodeInfo: []*source.NodeInfoResult{
					{UID: "node-1", Node: "node-a"},
				},
				source.QueryNodeUptime: []*source.UptimeResult{
					{UID: "node-1", First: start, Last: end},
				},
				source.QueryNodeResourceCapacities: []*source.ResourceResult{
					{UID: "unknown-node", Resource: "cpu", Unit: "cores", Value: 4.0},
				},
			},
			want: map[string]*kubemodel.Node{
				"node-1": {
					UID: "node-1", Name: "node-a",
					Start:                start,
					End:                  end,
					ResourceCapacities:   kubemodel.ResourceQuantities{},
					ResourcesAllocatable: kubemodel.ResourceQuantities{},
				},
			},
		},
		{
			name: "cpu usage data is populated via resource capacities",
			overrides: map[string]any{
				source.QueryNodeInfo: []*source.NodeInfoResult{
					{UID: "node-1", Node: "node-a"},
					{UID: "node-2", Node: "node-b"},
				},
				source.QueryNodeUptime: []*source.UptimeResult{
					{UID: "node-1", First: start, Last: end},
					{UID: "node-2", First: start, Last: end},
				},
				source.QueryKMLocalStorageUsedAvg: []*source.NodeUIDValueResult{
					{UID: "node-1", Value: 50 * 1024 * 1024 * 1024},
				},
				source.QueryKMLocalStorageUsedMax: []*source.NodeUIDValueResult{
					{UID: "node-2", Value: 75 * 1024 * 1024 * 1024},
				},
			},
			want: map[string]*kubemodel.Node{
				"node-1": {
					UID: "node-1", Name: "node-a",
					Start: start, End: end,
					FileSystem:           kubemodel.FileSystem{UsageByteAvg: 50 * 1024 * 1024 * 1024},
					ResourceCapacities:   kubemodel.ResourceQuantities{},
					ResourcesAllocatable: kubemodel.ResourceQuantities{},
				},
				"node-2": {
					UID: "node-2", Name: "node-b",
					Start: start, End: end,
					FileSystem:           kubemodel.FileSystem{UsageByteMax: 75 * 1024 * 1024 * 1024},
					ResourceCapacities:   kubemodel.ResourceQuantities{},
					ResourcesAllocatable: kubemodel.ResourceQuantities{},
				},
			},
		},
		{
			name: "gpu count via resource capacities",
			overrides: map[string]any{
				source.QueryNodeInfo: []*source.NodeInfoResult{
					{UID: "node-1", Node: "gpu-node"},
				},
				source.QueryNodeUptime: []*source.UptimeResult{
					{UID: "node-1", First: start, Last: end},
				},
				source.QueryNodeResourceCapacities: []*source.ResourceResult{
					{UID: "node-1", Resource: "nvidia.com/gpu", Unit: "count", Value: 8},
				},
			},
			want: map[string]*kubemodel.Node{
				"node-1": {
					UID: "node-1", Name: "gpu-node",
					Start: start, End: end,
					ResourceCapacities: kubemodel.ResourceQuantities{
						kubemodel.ResourceNvidia: {
							Resource: kubemodel.ResourceNvidia,
							Unit:     "count",
							Values:   kubemodel.Stats{kubemodel.StatAvg: 8},
						},
					},
					ResourcesAllocatable: kubemodel.ResourceQuantities{},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ds := source.NewMockOpenCostDataSource()
			ds.ResolutionValue = 5 * time.Minute
			seedCluster(ds, start, end)
			for method, result := range tt.overrides {
				ds.Querier.SetOverride(method, result)
			}

			km, err := NewKubeModel(testClusterUID, ds)
			require.NoError(t, err)

			kms, err := km.ComputeKubeModelSet(start, end)
			require.NoError(t, err)

			assert.Equal(t, tt.want, kms.Nodes)
		})
	}
}
