package kubemodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util"
)

func TestComputeContainers(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)

	tests := []struct {
		name      string
		overrides map[string]any
		want      map[string]*kubemodel.Container
	}{
		{
			name:      "no data returns empty container map",
			overrides: map[string]any{},
			want:      map[string]*kubemodel.Container{},
		},
		{
			name: "basic container uptime",
			overrides: map[string]any{
				source.QueryContainerUptime: []*source.ContainerUptimeResult{
					{UptimeResult: source.UptimeResult{UID: "pod-1", First: start, Last: end}, Container: "main"},
				},
			},
			want: map[string]*kubemodel.Container{
				"pod-1/main": {
					PodUID:           "pod-1",
					Name:             "main",
					Start:            start,
					End:              end,
					ResourceRequests: kubemodel.ResourceQuantities{},
					ResourceLimits:   kubemodel.ResourceQuantities{},
				},
			},
		},
		{
			name: "multiple containers on same pod",
			overrides: map[string]any{
				source.QueryContainerUptime: []*source.ContainerUptimeResult{
					{UptimeResult: source.UptimeResult{UID: "pod-1", First: start, Last: end}, Container: "main"},
					{UptimeResult: source.UptimeResult{UID: "pod-1", First: start, Last: end}, Container: "sidecar"},
				},
			},
			want: map[string]*kubemodel.Container{
				"pod-1/main": {
					PodUID:           "pod-1",
					Name:             "main",
					Start:            start,
					End:              end,
					ResourceRequests: kubemodel.ResourceQuantities{},
					ResourceLimits:   kubemodel.ResourceQuantities{},
				},
				"pod-1/sidecar": {
					PodUID:           "pod-1",
					Name:             "sidecar",
					Start:            start,
					End:              end,
					ResourceRequests: kubemodel.ResourceQuantities{},
					ResourceLimits:   kubemodel.ResourceQuantities{},
				},
			},
		},
		{
			name: "resource requests and limits are populated",
			overrides: map[string]any{
				source.QueryContainerUptime: []*source.ContainerUptimeResult{
					{UptimeResult: source.UptimeResult{UID: "pod-1", First: start, Last: end}, Container: "main"},
				},
				source.QueryContainerResourceRequests: []*source.ContainerResourceResult{
					{ResourceResult: source.ResourceResult{UID: "pod-1", Resource: "cpu", Unit: "cores", Value: 0.5}, Container: "main"},
					{ResourceResult: source.ResourceResult{UID: "pod-1", Resource: "memory", Unit: "bytes", Value: 512 * 1024 * 1024}, Container: "main"},
				},
				source.QueryContainerResourceLimits: []*source.ContainerResourceResult{
					{ResourceResult: source.ResourceResult{UID: "pod-1", Resource: "cpu", Unit: "cores", Value: 1.0}, Container: "main"},
					{ResourceResult: source.ResourceResult{UID: "pod-1", Resource: "memory", Unit: "bytes", Value: 1024 * 1024 * 1024}, Container: "main"},
				},
			},
			want: map[string]*kubemodel.Container{
				"pod-1/main": {
					PodUID: "pod-1",
					Name:   "main",
					Start:  start,
					End:    end,
					ResourceRequests: kubemodel.ResourceQuantities{
						kubemodel.ResourceCPU: {
							Resource: kubemodel.ResourceCPU,
							Unit:     kubemodel.UnitCore,
							Values:   kubemodel.Stats{kubemodel.StatAvg: 0.5},
						},
						kubemodel.ResourceMemory: {
							Resource: kubemodel.ResourceMemory,
							Unit:     kubemodel.UnitByte,
							Values:   kubemodel.Stats{kubemodel.StatAvg: 512 * 1024 * 1024},
						},
					},
					ResourceLimits: kubemodel.ResourceQuantities{
						kubemodel.ResourceCPU: {
							Resource: kubemodel.ResourceCPU,
							Unit:     kubemodel.UnitCore,
							Values:   kubemodel.Stats{kubemodel.StatAvg: 1.0},
						},
						kubemodel.ResourceMemory: {
							Resource: kubemodel.ResourceMemory,
							Unit:     kubemodel.UnitByte,
							Values:   kubemodel.Stats{kubemodel.StatAvg: 1024 * 1024 * 1024},
						},
					},
				},
			},
		},
		{
			name: "cpu and ram allocation and usage are populated",
			overrides: map[string]any{
				source.QueryContainerUptime: []*source.ContainerUptimeResult{
					{UptimeResult: source.UptimeResult{UID: "pod-1", First: start, Last: end}, Container: "main"},
				},
				source.QueryCPUCoresAllocated: []*source.CPUCoresAllocatedResult{
					{UID: "pod-1", Container: "main", Data: []*util.Vector{{Value: 0.25}}},
				},
				source.QueryRAMBytesAllocated: []*source.RAMBytesAllocatedResult{
					{UID: "pod-1", Container: "main", Data: []*util.Vector{{Value: 256 * 1024 * 1024}}},
				},
				source.QueryCPUUsageAvg: []*source.CPUUsageAvgResult{
					{UID: "pod-1", Container: "main", Data: []*util.Vector{{Value: 0.1}}},
				},
				source.QueryCPUUsageMax: []*source.CPUUsageMaxResult{
					{UID: "pod-1", Container: "main", Data: []*util.Vector{{Value: 0.2}}},
				},
				source.QueryRAMUsageAvg: []*source.RAMUsageAvgResult{
					{UID: "pod-1", Container: "main", Data: []*util.Vector{{Value: 128 * 1024 * 1024}}},
				},
				source.QueryRAMUsageMax: []*source.RAMUsageMaxResult{
					{UID: "pod-1", Container: "main", Data: []*util.Vector{{Value: 200 * 1024 * 1024}}},
				},
			},
			want: map[string]*kubemodel.Container{
				"pod-1/main": {
					PodUID:                "pod-1",
					Name:                  "main",
					Start:                 start,
					End:                   end,
					ResourceRequests:      kubemodel.ResourceQuantities{},
					ResourceLimits:        kubemodel.ResourceQuantities{},
					CPUCoreAllocationAvg:  0.25,
					RAMBytesAllocationAvg: 256 * 1024 * 1024,
					CPUCoreUsageAvg:       0.1,
					CPUCoreUsageMax:       0.2,
					RAMBytesUsageAvg:      128 * 1024 * 1024,
					RAMBytesUsageMax:      200 * 1024 * 1024,
				},
			},
		},
		{
			name: "device usage avg and max are populated",
			overrides: map[string]any{
				source.QueryContainerUptime: []*source.ContainerUptimeResult{
					{UptimeResult: source.UptimeResult{UID: "pod-1", First: start, Last: end}, Container: "training"},
				},
				source.QueryDCGMContainerUsageAvg: []*source.DCGMDeviceContainerUsageResult{
					{UUID: "GPU-abc123", PodUID: "pod-1", Container: "training", Value: 0.75},
				},
				source.QueryDCGMContainerUsageMax: []*source.DCGMDeviceContainerUsageResult{
					{UUID: "GPU-abc123", PodUID: "pod-1", Container: "training", Value: 0.95},
				},
			},
			want: map[string]*kubemodel.Container{
				"pod-1/training": {
					PodUID:           "pod-1",
					Name:             "training",
					Start:            start,
					End:              end,
					ResourceRequests: kubemodel.ResourceQuantities{},
					ResourceLimits:   kubemodel.ResourceQuantities{},
					DeviceUsages: map[string]kubemodel.DeviceUsage{
						"GPU-abc123": {UsageAvg: 0.75, UsageMax: 0.95},
					},
				},
			},
		},
		{
			name: "device usage with empty pod uid or container is ignored",
			overrides: map[string]any{
				source.QueryContainerUptime: []*source.ContainerUptimeResult{
					{UptimeResult: source.UptimeResult{UID: "pod-1", First: start, Last: end}, Container: "training"},
				},
				source.QueryDCGMContainerUsageAvg: []*source.DCGMDeviceContainerUsageResult{
					{UUID: "GPU-abc123", PodUID: "", Container: "training", Value: 0.5},
					{UUID: "GPU-abc123", PodUID: "pod-1", Container: "", Value: 0.5},
				},
			},
			want: map[string]*kubemodel.Container{
				"pod-1/training": {
					PodUID:           "pod-1",
					Name:             "training",
					Start:            start,
					End:              end,
					ResourceRequests: kubemodel.ResourceQuantities{},
					ResourceLimits:   kubemodel.ResourceQuantities{},
				},
			},
		},
		{
			name: "device usage for unknown container is ignored",
			overrides: map[string]any{
				source.QueryContainerUptime: []*source.ContainerUptimeResult{
					{UptimeResult: source.UptimeResult{UID: "pod-1", First: start, Last: end}, Container: "main"},
				},
				source.QueryDCGMContainerUsageAvg: []*source.DCGMDeviceContainerUsageResult{
					{UUID: "GPU-abc123", PodUID: "pod-1", Container: "training", Value: 0.75},
				},
			},
			want: map[string]*kubemodel.Container{
				"pod-1/main": {
					PodUID:           "pod-1",
					Name:             "main",
					Start:            start,
					End:              end,
					ResourceRequests: kubemodel.ResourceQuantities{},
					ResourceLimits:   kubemodel.ResourceQuantities{},
				},
			},
		},
		{
			name: "resource requests for unknown container are ignored",
			overrides: map[string]any{
				source.QueryContainerUptime: []*source.ContainerUptimeResult{
					{UptimeResult: source.UptimeResult{UID: "pod-1", First: start, Last: end}, Container: "main"},
				},
				source.QueryContainerResourceRequests: []*source.ContainerResourceResult{
					{ResourceResult: source.ResourceResult{UID: "pod-1", Resource: "cpu", Unit: "cores", Value: 0.5}, Container: "unknown-container"},
				},
			},
			want: map[string]*kubemodel.Container{
				"pod-1/main": {
					PodUID:           "pod-1",
					Name:             "main",
					Start:            start,
					End:              end,
					ResourceRequests: kubemodel.ResourceQuantities{},
					ResourceLimits:   kubemodel.ResourceQuantities{},
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

			km, err := NewKubeModel(testClusterUID, false, ds)
			require.NoError(t, err)

			kms, err := km.ComputeKubeModelSet(start, end)
			require.NoError(t, err)

			assert.Equal(t, tt.want, kms.Containers)
		})
	}
}
