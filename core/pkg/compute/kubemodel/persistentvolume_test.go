package kubemodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
)

func TestComputePersistentVolumes(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)

	tests := []struct {
		name      string
		overrides map[string]any
		want      map[string]*kubemodel.PersistentVolume
	}{
		{
			name:      "no data returns empty pv map",
			overrides: map[string]any{},
			want:      map[string]*kubemodel.PersistentVolume{},
		},
		{
			name: "basic pv info and uptime",
			overrides: map[string]any{
				source.QueryKMPVInfo: []*source.PVInfoResult{
					{UID: "pv-1", PersistentVolume: "pvc-data-0"},
				},
				source.QueryPVUptime: []*source.UptimeResult{
					{UID: "pv-1", First: start, Last: end},
				},
			},
			want: map[string]*kubemodel.PersistentVolume{
				"pv-1": {
					UID:   "pv-1",
					Name:  "pvc-data-0",
					Start: start,
					End:   end,
				},
			},
		},
		{
			name: "pv without uptime is not registered",
			overrides: map[string]any{
				source.QueryKMPVInfo: []*source.PVInfoResult{
					{UID: "pv-1", PersistentVolume: "pvc-data-0"},
				},
			},
			want: map[string]*kubemodel.PersistentVolume{},
		},
		{
			name: "pv with storage class and provider id",
			overrides: map[string]any{
				source.QueryKMPVInfo: []*source.PVInfoResult{
					{UID: "pv-1", PersistentVolume: "pvc-data-0", StorageClass: "gp2", ProviderID: "vol-abc123"},
				},
				source.QueryPVUptime: []*source.UptimeResult{
					{UID: "pv-1", First: start, Last: end},
				},
			},
			want: map[string]*kubemodel.PersistentVolume{
				"pv-1": {
					UID:          "pv-1",
					Name:         "pvc-data-0",
					StorageClass: "gp2",
					ProviderID:   "vol-abc123",
					Start:        start,
					End:          end,
				},
			},
		},
		{
			name: "pv with csi volume handle",
			overrides: map[string]any{
				source.QueryKMPVInfo: []*source.PVInfoResult{
					{UID: "pv-1", PersistentVolume: "pvc-data-0", StorageClass: "gp2", CSIVolumeHandle: "vol-abc123"},
				},
				source.QueryPVUptime: []*source.UptimeResult{
					{UID: "pv-1", First: start, Last: end},
				},
			},
			want: map[string]*kubemodel.PersistentVolume{
				"pv-1": {
					UID:          "pv-1",
					Name:         "pvc-data-0",
					StorageClass: "gp2",
					ProviderID:   "",
					Start:        start,
					End:          end,
				},
			},
		},
		{
			name: "pv size bytes is populated",
			overrides: map[string]any{
				source.QueryKMPVInfo: []*source.PVInfoResult{
					{UID: "pv-1", PersistentVolume: "pvc-data-0"},
				},
				source.QueryPVUptime: []*source.UptimeResult{
					{UID: "pv-1", First: start, Last: end},
				},
				source.QueryPVBytes: []*source.PVBytesResult{
					{UID: "pv-1", Value: 10 * 1024 * 1024 * 1024},
				},
			},
			want: map[string]*kubemodel.PersistentVolume{
				"pv-1": {
					UID:       "pv-1",
					Name:      "pvc-data-0",
					Start:     start,
					End:       end,
					SizeBytes: 10 * 1024 * 1024 * 1024,
				},
			},
		},
		{
			name: "pv bytes for unknown pv is ignored",
			overrides: map[string]any{
				source.QueryKMPVInfo: []*source.PVInfoResult{
					{UID: "pv-1", PersistentVolume: "pvc-data-0"},
				},
				source.QueryPVUptime: []*source.UptimeResult{
					{UID: "pv-1", First: start, Last: end},
				},
				source.QueryPVBytes: []*source.PVBytesResult{
					{UID: "unknown-pv", Value: 999},
				},
			},
			want: map[string]*kubemodel.PersistentVolume{
				"pv-1": {
					UID:   "pv-1",
					Name:  "pvc-data-0",
					Start: start,
					End:   end,
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

			assert.Equal(t, tt.want, kms.PersistentVolumes)
		})
	}
}

func TestComputePersistentVolumes_LocalPVs(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)

	tests := []struct {
		name             string
		includeLocalDisk string
		wantUIDs         []string
	}{
		{
			name:             "local PVs included by default",
			includeLocalDisk: "true",
			wantUIDs:         []string{"pv-1", "pv-2"},
		},
		{
			name:             "local PVs excluded when local disk cost is disabled",
			includeLocalDisk: "false",
			wantUIDs:         []string{"pv-1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(env.AssetIncludeLocalDiskCostEnvVar, tt.includeLocalDisk)

			ds := source.NewMockOpenCostDataSource()
			ds.ResolutionValue = 5 * time.Minute
			seedCluster(ds, start, end)
			ds.Querier.SetOverride(source.QueryKMPVInfo, []*source.PVInfoResult{
				{UID: "pv-1", PersistentVolume: "pvc-data-0"},
				{UID: "pv-2", PersistentVolume: "local-pv-1a2b3c4d"},
			})
			ds.Querier.SetOverride(source.QueryPVUptime, []*source.UptimeResult{
				{UID: "pv-1", First: start, Last: end},
				{UID: "pv-2", First: start, Last: end},
			})
			ds.Querier.SetOverride(source.QueryPVBytes, []*source.PVBytesResult{
				{UID: "pv-1", Value: 10 * 1024 * 1024 * 1024},
				{UID: "pv-2", Value: 20 * 1024 * 1024 * 1024},
			})

			km, err := NewKubeModel(testClusterUID, false, ds)
			require.NoError(t, err)

			kms, err := km.ComputeKubeModelSet(start, end)
			require.NoError(t, err)

			got := make([]string, 0, len(kms.PersistentVolumes))
			for uid := range kms.PersistentVolumes {
				got = append(got, uid)
			}
			assert.ElementsMatch(t, tt.wantUIDs, got)
		})
	}
}
