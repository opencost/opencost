package kubemodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
)

func TestComputeDevices(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)

	tests := []struct {
		name         string
		overrides    map[string]any
		containers   map[string]*kubemodel.Container
		wantDevices  map[string]*kubemodel.Device
		wantUsageKey string
		wantUsage    *kubemodel.DeviceUsage
	}{
		{
			name:        "no data returns empty device map",
			overrides:   map[string]any{},
			wantDevices: map[string]*kubemodel.Device{},
		},
		{
			name: "basic device info and uptime",
			overrides: map[string]any{
				source.QueryDCGMDeviceInfo: []*source.DCGMDeviceInfoResult{
					{UUID: "GPU-abc123", Device: "nvidia0", ModelName: "A100"},
				},
				source.QueryDCGMDeviceUptime: []*source.DCGMDeviceUptimeResult{
					{UUID: "GPU-abc123", First: start, Last: end},
				},
			},
			wantDevices: map[string]*kubemodel.Device{
				"GPU-abc123": {
					UUID:      "GPU-abc123",
					Device:    "nvidia0",
					ModelName: "A100",
					Start:     start,
					End:       end,
				},
			},
		},
		{
			name: "device without uptime is not registered",
			overrides: map[string]any{
				source.QueryDCGMDeviceInfo: []*source.DCGMDeviceInfoResult{
					{UUID: "GPU-abc123", Device: "nvidia0", ModelName: "A100"},
				},
			},
			wantDevices: map[string]*kubemodel.Device{},
		},
		{
			name: "device with empty uuid is skipped",
			overrides: map[string]any{
				source.QueryDCGMDeviceInfo: []*source.DCGMDeviceInfoResult{
					{UUID: "", Device: "nvidia0", ModelName: "A100"},
				},
				source.QueryDCGMDeviceUptime: []*source.DCGMDeviceUptimeResult{
					{UUID: "GPU-abc123", First: start, Last: end},
				},
			},
			wantDevices: map[string]*kubemodel.Device{},
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
			wantDevices: map[string]*kubemodel.Device{
				"GPU-abc123": {
					UUID:      "GPU-abc123",
					Device:    "nvidia0",
					ModelName: "A100",
					Start:     start,
					End:       end,
				},
			},
		},
		{
			name: "container usage avg and max are applied to a registered container",
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
			containers: map[string]*kubemodel.Container{
				"pod-1/training": {PodUID: "pod-1", Name: "training"},
			},
			wantDevices: map[string]*kubemodel.Device{
				"GPU-abc123": {
					UUID:      "GPU-abc123",
					Device:    "nvidia0",
					ModelName: "A100",
					Start:     start,
					End:       end,
				},
			},
			wantUsageKey: "pod-1/training",
			wantUsage:    &kubemodel.DeviceUsage{UsageAvg: 0.75, UsageMax: 0.95},
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
			containers: map[string]*kubemodel.Container{
				"pod-1/training": {PodUID: "pod-1", Name: "training"},
			},
			wantDevices: map[string]*kubemodel.Device{
				"GPU-abc123": {
					UUID:      "GPU-abc123",
					Device:    "nvidia0",
					ModelName: "A100",
					Start:     start,
					End:       end,
				},
			},
			wantUsageKey: "pod-1/training",
			wantUsage:    nil,
		},
		{
			name: "usage for an unregistered container is ignored",
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
			},
			wantDevices: map[string]*kubemodel.Device{
				"GPU-abc123": {
					UUID:      "GPU-abc123",
					Device:    "nvidia0",
					ModelName: "A100",
					Start:     start,
					End:       end,
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

			kms := kubemodel.NewKubeModelSet(start, end)
			if tt.containers != nil {
				kms.Containers = tt.containers
			}

			err = km.computeDevices(kms, start, end)
			require.NoError(t, err)

			assert.Equal(t, tt.wantDevices, kms.Devices)

			if tt.wantUsageKey != "" {
				c, ok := kms.Containers[tt.wantUsageKey]
				require.True(t, ok)
				if tt.wantUsage == nil {
					assert.Empty(t, c.DeviceUsages)
				} else {
					require.NotNil(t, c.DeviceUsages)
					assert.Equal(t, *tt.wantUsage, c.DeviceUsages["GPU-abc123"])
				}
			}
		})
	}
}
