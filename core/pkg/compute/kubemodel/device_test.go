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
		name      string
		overrides map[string]any
		want      map[string]*kubemodel.Device
	}{
		{
			name:      "no data returns empty device map",
			overrides: map[string]any{},
			want:      map[string]*kubemodel.Device{},
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
			want: map[string]*kubemodel.Device{
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
			want: map[string]*kubemodel.Device{},
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
			want: map[string]*kubemodel.Device{},
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
			want: map[string]*kubemodel.Device{
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

			err = km.computeDevices(kms, start, end)
			require.NoError(t, err)

			assert.Equal(t, tt.want, kms.Devices)
		})
	}
}
