package kubemodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/opencost/opencost/core/pkg/cloud"
	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
)

func TestComputeCluster(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)

	tests := []struct {
		name      string
		overrides map[string]any
		want      *kubemodel.Cluster
		wantErr   bool
	}{
		{
			name:      "no data returns error",
			overrides: map[string]any{},
			wantErr:   true,
		},
		{
			name: "basic cluster info and uptime",
			overrides: map[string]any{
				source.QueryClusterInfo: []*source.ClusterInfoResult{
					{UID: testClusterUID, Cluster: "my-cluster"},
				},
				source.QueryClusterUptime: []*source.UptimeResult{
					{UID: testClusterUID, First: start, Last: end},
				},
			},
			want: &kubemodel.Cluster{
				UID:   testClusterUID,
				Name:  "my-cluster",
				Start: start,
				End:   end,
			},
		},
		{
			name: "cluster with provider, account, and region",
			overrides: map[string]any{
				source.QueryClusterInfo: []*source.ClusterInfoResult{
					{UID: testClusterUID, Cluster: "prod-cluster", Provider: "aws", AccountID: "123456789", Region: "us-east-1"},
				},
				source.QueryClusterUptime: []*source.UptimeResult{
					{UID: testClusterUID, First: start, Last: end},
				},
			},
			want: &kubemodel.Cluster{
				UID:      testClusterUID,
				Name:     "prod-cluster",
				Provider: cloud.ProviderAWS,
				Account:  "123456789",
				Region:   "us-east-1",
				Start:    start,
				End:      end,
			},
		},
		{
			name: "cluster without uptime is registered with zero window but fails validation",
			overrides: map[string]any{
				source.QueryClusterInfo: []*source.ClusterInfoResult{
					{UID: testClusterUID, Cluster: "my-cluster"},
				},
			},
			want: nil,
		},
		{
			name: "uptime for unknown cluster is ignored",
			overrides: map[string]any{
				source.QueryClusterInfo: []*source.ClusterInfoResult{
					{UID: testClusterUID, Cluster: "my-cluster"},
				},
				source.QueryClusterUptime: []*source.UptimeResult{
					{UID: testClusterUID, First: start, Last: end},
					{UID: "unknown-cluster", First: start, Last: end},
				},
			},
			want: &kubemodel.Cluster{
				UID:   testClusterUID,
				Name:  "my-cluster",
				Start: start,
				End:   end,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ds := source.NewMockOpenCostDataSource()
			ds.ResolutionValue = 5 * time.Minute
			for method, result := range tt.overrides {
				ds.Querier.SetOverride(method, result)
			}

			km, err := NewKubeModel(testClusterUID, false, ds)
			require.NoError(t, err)

			kms, err := km.ComputeKubeModelSet(start, end)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, kms.Cluster)
		})
	}
}
