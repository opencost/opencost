package kubemodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
)

func TestComputeDCGMDevices(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)

	tests := []struct {
		name      string
		overrides map[string]any
		want      map[string]*kubemodel.DCGMDevice
	}{
		{
			name:      "no data returns empty dcgm device map",
			overrides: map[string]any{},
			want:      map[string]*kubemodel.DCGMDevice{},
		},
		{
			name: "basic dcgm device info and uptime",
			overrides: map[string]any{
				source.QueryDCGMDeviceInfo: []*source.DCGMDeviceInfoResult{
					{UUID: "GPU-abc123", Device: "nvidia0", ModelName: "A100"},
				},
				source.QueryDCGMDeviceUptime: []*source.DCGMDeviceUptimeResult{
					{UUID: "GPU-abc123", First: start, Last: end},
				},
			},
			want: map[string]*kubemodel.DCGMDevice{
				"GPU-abc123": {
					UUID:      "GPU-abc123",
					Device:    "nvidia0",
					ModelName: "A100",
					Start:     start,
					End:       end,
					PodUsages: map[string]kubemodel.DCGMPod{},
				},
			},
		},
		{
			name: "dcgm device without uptime is not registered",
			overrides: map[string]any{
				source.QueryDCGMDeviceInfo: []*source.DCGMDeviceInfoResult{
					{UUID: "GPU-abc123", Device: "nvidia0", ModelName: "A100"},
				},
			},
			want: map[string]*kubemodel.DCGMDevice{},
		},
		{
			name: "dcgm device with empty uuid is skipped",
			overrides: map[string]any{
				source.QueryDCGMDeviceInfo: []*source.DCGMDeviceInfoResult{
					{UUID: "", Device: "nvidia0", ModelName: "A100"},
				},
				source.QueryDCGMDeviceUptime: []*source.DCGMDeviceUptimeResult{
					{UUID: "GPU-abc123", First: start, Last: end},
				},
			},
			want: map[string]*kubemodel.DCGMDevice{},
		},
		{
			name: "dcgm container usage avg and max are populated",
			overrides: map[string]any{
				source.QueryDCGMDeviceInfo: []*source.DCGMDeviceInfoResult{
					{UUID: "GPU-abc123", Device: "nvidia0", ModelName: "A100"},
				},
				source.QueryDCGMDeviceUptime: []*source.DCGMDeviceUptimeResult{
					{UUID: "GPU-abc123", First: start, Last: end},
				},
				source.QueryDCGMContainerUsageAvg: []*source.DCGMDeviceContainerUsageResult{
					{UUID: "GPU-abc123", PodUID: "pod-1", Container: "training", Value: 0.75},
				},
				source.QueryDCGMContainerUsageMax: []*source.DCGMDeviceContainerUsageResult{
					{UUID: "GPU-abc123", PodUID: "pod-1", Container: "training", Value: 0.95},
				},
			},
			want: map[string]*kubemodel.DCGMDevice{
				"GPU-abc123": {
					UUID:      "GPU-abc123",
					Device:    "nvidia0",
					ModelName: "A100",
					Start:     start,
					End:       end,
					PodUsages: map[string]kubemodel.DCGMPod{
						"pod-1": {
							ContainerUsages: map[string]kubemodel.DCGMContainer{
								"training": {UsageAvg: 0.75, UsageMax: 0.95},
							},
						},
					},
				},
			},
		},
		{
			name: "usage with empty pod uid or container is ignored",
			overrides: map[string]any{
				source.QueryDCGMDeviceInfo: []*source.DCGMDeviceInfoResult{
					{UUID: "GPU-abc123", Device: "nvidia0", ModelName: "A100"},
				},
				source.QueryDCGMDeviceUptime: []*source.DCGMDeviceUptimeResult{
					{UUID: "GPU-abc123", First: start, Last: end},
				},
				source.QueryDCGMContainerUsageAvg: []*source.DCGMDeviceContainerUsageResult{
					{UUID: "GPU-abc123", PodUID: "", Container: "training", Value: 0.5},
					{UUID: "GPU-abc123", PodUID: "pod-1", Container: "", Value: 0.5},
				},
			},
			want: map[string]*kubemodel.DCGMDevice{
				"GPU-abc123": {
					UUID:      "GPU-abc123",
					Device:    "nvidia0",
					ModelName: "A100",
					Start:     start,
					End:       end,
					PodUsages: map[string]kubemodel.DCGMPod{},
				},
			},
		},
		{
			name: "duplicate device info entries use first occurrence",
			overrides: map[string]any{
				source.QueryDCGMDeviceInfo: []*source.DCGMDeviceInfoResult{
					{UUID: "GPU-abc123", Device: "nvidia0", ModelName: "A100"},
					{UUID: "GPU-abc123", Device: "nvidia0-dup", ModelName: "A100-dup"},
				},
				source.QueryDCGMDeviceUptime: []*source.DCGMDeviceUptimeResult{
					{UUID: "GPU-abc123", First: start, Last: end},
				},
			},
			want: map[string]*kubemodel.DCGMDevice{
				"GPU-abc123": {
					UUID:      "GPU-abc123",
					Device:    "nvidia0",
					ModelName: "A100",
					Start:     start,
					End:       end,
					PodUsages: map[string]kubemodel.DCGMPod{},
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

			assert.Equal(t, tt.want, kms.DCGMDevices)
		})
	}
}
