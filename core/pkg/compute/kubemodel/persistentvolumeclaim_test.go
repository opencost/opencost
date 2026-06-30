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

func TestComputePersistentVolumeClaims(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)

	tests := []struct {
		name      string
		overrides map[string]any
		want      map[string]*kubemodel.PersistentVolumeClaim
	}{
		{
			name:      "no data returns empty pvc map",
			overrides: map[string]any{},
			want:      map[string]*kubemodel.PersistentVolumeClaim{},
		},
		{
			name: "basic pvc info and uptime",
			overrides: map[string]any{
				source.QueryKMPVCInfo: []*source.PVCInfoResult{
					{UID: "pvc-1", PersistentVolumeClaim: "data-claim", NamespaceUID: "ns-1"},
				},
				source.QueryPVCUptime: []*source.UptimeResult{
					{UID: "pvc-1", First: start, Last: end},
				},
			},
			want: map[string]*kubemodel.PersistentVolumeClaim{
				"pvc-1": {
					UID:          "pvc-1",
					Name:         "data-claim",
					NamespaceUID: "ns-1",
					Start:        start,
					End:          end,
				},
			},
		},
		{
			name: "pvc without uptime is not registered",
			overrides: map[string]any{
				source.QueryKMPVCInfo: []*source.PVCInfoResult{
					{UID: "pvc-1", PersistentVolumeClaim: "data-claim", NamespaceUID: "ns-1"},
				},
			},
			want: map[string]*kubemodel.PersistentVolumeClaim{},
		},
		{
			name: "pvc with pv uid and storage class",
			overrides: map[string]any{
				source.QueryKMPVCInfo: []*source.PVCInfoResult{
					{UID: "pvc-1", PersistentVolumeClaim: "data-claim", NamespaceUID: "ns-1", PVUID: "pv-1", StorageClass: "gp2"},
				},
				source.QueryPVCUptime: []*source.UptimeResult{
					{UID: "pvc-1", First: start, Last: end},
				},
			},
			want: map[string]*kubemodel.PersistentVolumeClaim{
				"pvc-1": {
					UID:                 "pvc-1",
					Name:                "data-claim",
					NamespaceUID:        "ns-1",
					PersistentVolumeUID: "pv-1",
					StorageClass:        "gp2",
					Start:               start,
					End:                 end,
				},
			},
		},
		{
			name: "pvc requested bytes and usage are populated",
			overrides: map[string]any{
				source.QueryKMPVCInfo: []*source.PVCInfoResult{
					{UID: "pvc-1", PersistentVolumeClaim: "data-claim", NamespaceUID: "ns-1"},
				},
				source.QueryPVCUptime: []*source.UptimeResult{
					{UID: "pvc-1", First: start, Last: end},
				},
				source.QueryPVCBytesRequested: []*source.PVCBytesRequestedResult{
					{UID: "pvc-1", Data: []*util.Vector{{Value: 5 * 1024 * 1024 * 1024}}},
				},
				source.QueryPVCBytesUsedAverage: []*source.PVCUIDValueResult{
					{UID: "pvc-1", Value: 2 * 1024 * 1024 * 1024},
				},
				source.QueryPVCBytesUsedMax: []*source.PVCUIDValueResult{
					{UID: "pvc-1", Value: 4 * 1024 * 1024 * 1024},
				},
			},
			want: map[string]*kubemodel.PersistentVolumeClaim{
				"pvc-1": {
					UID:            "pvc-1",
					Name:           "data-claim",
					NamespaceUID:   "ns-1",
					Start:          start,
					End:            end,
					RequestedBytes: 5 * 1024 * 1024 * 1024,
					UsageBytesAvg:  2 * 1024 * 1024 * 1024,
					UsageBytesMax:  4 * 1024 * 1024 * 1024,
				},
			},
		},
		{
			name: "bytes for unknown pvc are ignored",
			overrides: map[string]any{
				source.QueryKMPVCInfo: []*source.PVCInfoResult{
					{UID: "pvc-1", PersistentVolumeClaim: "data-claim", NamespaceUID: "ns-1"},
				},
				source.QueryPVCUptime: []*source.UptimeResult{
					{UID: "pvc-1", First: start, Last: end},
				},
				source.QueryPVCBytesUsedAverage: []*source.PVCUIDValueResult{
					{UID: "unknown-pvc", Value: 999},
				},
			},
			want: map[string]*kubemodel.PersistentVolumeClaim{
				"pvc-1": {
					UID:          "pvc-1",
					Name:         "data-claim",
					NamespaceUID: "ns-1",
					Start:        start,
					End:          end,
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

			assert.Equal(t, tt.want, kms.PersistentVolumeClaims)
		})
	}
}
