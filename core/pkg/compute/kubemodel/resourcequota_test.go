package kubemodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
)

func TestComputeResourceQuotas(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)

	emptyRQ := func(uid, name, nsUID string) *kubemodel.ResourceQuota {
		return &kubemodel.ResourceQuota{
			UID:          uid,
			Name:         name,
			NamespaceUID: nsUID,
			Start:        start,
			End:          end,
			Spec:         &kubemodel.ResourceQuotaSpec{Hard: &kubemodel.ResourceQuotaSpecHard{}},
			Status:       &kubemodel.ResourceQuotaStatus{Used: &kubemodel.ResourceQuotaStatusUsed{}},
		}
	}

	tests := []struct {
		name      string
		overrides map[string]any
		want      map[string]*kubemodel.ResourceQuota
	}{
		{
			name:      "no data returns empty resource quota map",
			overrides: map[string]any{},
			want:      map[string]*kubemodel.ResourceQuota{},
		},
		{
			name: "basic resource quota info and uptime",
			overrides: map[string]any{
				source.QueryResourceQuotaInfo: []*source.ResourceQuotaInfoResult{
					{UID: "rq-1", ResourceQuota: "compute-quota", NamespaceUID: "ns-1"},
				},
				source.QueryResourceQuotaUptime: []*source.UptimeResult{
					{UID: "rq-1", First: start, Last: end},
				},
			},
			want: map[string]*kubemodel.ResourceQuota{
				"rq-1": emptyRQ("rq-1", "compute-quota", "ns-1"),
			},
		},
		{
			name: "resource quota without uptime is not registered",
			overrides: map[string]any{
				source.QueryResourceQuotaInfo: []*source.ResourceQuotaInfoResult{
					{UID: "rq-1", ResourceQuota: "compute-quota", NamespaceUID: "ns-1"},
				},
			},
			want: map[string]*kubemodel.ResourceQuota{},
		},
		{
			name: "spec cpu and ram request limits are populated",
			overrides: map[string]any{
				source.QueryResourceQuotaInfo: []*source.ResourceQuotaInfoResult{
					{UID: "rq-1", ResourceQuota: "compute-quota", NamespaceUID: "ns-1"},
				},
				source.QueryResourceQuotaUptime: []*source.UptimeResult{
					{UID: "rq-1", First: start, Last: end},
				},
				source.QueryResourceQuotaSpecCPURequestAverage: []*source.ResourceResult{
					{UID: "rq-1", Value: 2.0},
				},
				source.QueryResourceQuotaSpecCPURequestMax: []*source.ResourceResult{
					{UID: "rq-1", Value: 4.0},
				},
				source.QueryResourceQuotaSpecRAMRequestAverage: []*source.ResourceResult{
					{UID: "rq-1", Value: 4 * 1024 * 1024 * 1024},
				},
				source.QueryResourceQuotaSpecRAMRequestMax: []*source.ResourceResult{
					{UID: "rq-1", Value: 8 * 1024 * 1024 * 1024},
				},
			},
			want: map[string]*kubemodel.ResourceQuota{
				"rq-1": {
					UID:          "rq-1",
					Name:         "compute-quota",
					NamespaceUID: "ns-1",
					Start:        start,
					End:          end,
					Spec: &kubemodel.ResourceQuotaSpec{
						Hard: &kubemodel.ResourceQuotaSpecHard{
							Requests: kubemodel.ResourceQuantities{
								kubemodel.ResourceCPU: {
									Resource: kubemodel.ResourceCPU,
									Unit:     kubemodel.UnitMillicore,
									Values:   kubemodel.Stats{kubemodel.StatAvg: 2000, kubemodel.StatMax: 4000},
								},
								kubemodel.ResourceMemory: {
									Resource: kubemodel.ResourceMemory,
									Unit:     kubemodel.UnitByte,
									Values:   kubemodel.Stats{kubemodel.StatAvg: 4 * 1024 * 1024 * 1024, kubemodel.StatMax: 8 * 1024 * 1024 * 1024},
								},
							},
						},
					},
					Status: &kubemodel.ResourceQuotaStatus{Used: &kubemodel.ResourceQuotaStatusUsed{}},
				},
			},
		},
		{
			name: "status used cpu and ram are populated",
			overrides: map[string]any{
				source.QueryResourceQuotaInfo: []*source.ResourceQuotaInfoResult{
					{UID: "rq-1", ResourceQuota: "compute-quota", NamespaceUID: "ns-1"},
				},
				source.QueryResourceQuotaUptime: []*source.UptimeResult{
					{UID: "rq-1", First: start, Last: end},
				},
				source.QueryResourceQuotaStatusUsedCPURequestAverage: []*source.ResourceResult{
					{UID: "rq-1", Value: 1.0},
				},
				source.QueryResourceQuotaStatusUsedRAMRequestAverage: []*source.ResourceResult{
					{UID: "rq-1", Value: 2 * 1024 * 1024 * 1024},
				},
			},
			want: map[string]*kubemodel.ResourceQuota{
				"rq-1": {
					UID:          "rq-1",
					Name:         "compute-quota",
					NamespaceUID: "ns-1",
					Start:        start,
					End:          end,
					Spec:         &kubemodel.ResourceQuotaSpec{Hard: &kubemodel.ResourceQuotaSpecHard{}},
					Status: &kubemodel.ResourceQuotaStatus{
						Used: &kubemodel.ResourceQuotaStatusUsed{
							Requests: kubemodel.ResourceQuantities{
								kubemodel.ResourceCPU: {
									Resource: kubemodel.ResourceCPU,
									Unit:     kubemodel.UnitMillicore,
									Values:   kubemodel.Stats{kubemodel.StatAvg: 1000},
								},
								kubemodel.ResourceMemory: {
									Resource: kubemodel.ResourceMemory,
									Unit:     kubemodel.UnitByte,
									Values:   kubemodel.Stats{kubemodel.StatAvg: 2 * 1024 * 1024 * 1024},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "uptime for unknown resource quota is ignored",
			overrides: map[string]any{
				source.QueryResourceQuotaInfo: []*source.ResourceQuotaInfoResult{
					{UID: "rq-1", ResourceQuota: "compute-quota", NamespaceUID: "ns-1"},
				},
				source.QueryResourceQuotaUptime: []*source.UptimeResult{
					{UID: "rq-1", First: start, Last: end},
					{UID: "unknown-rq", First: start, Last: end},
				},
			},
			want: map[string]*kubemodel.ResourceQuota{
				"rq-1": emptyRQ("rq-1", "compute-quota", "ns-1"),
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

			assert.Equal(t, tt.want, kms.ResourceQuotas)
		})
	}
}