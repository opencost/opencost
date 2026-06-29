package kubemodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
)

func TestComputeNamespaces(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)

	tests := []struct {
		name      string
		overrides map[string]any
		want      map[string]*kubemodel.Namespace
	}{
		{
			name:      "no data returns empty namespace map",
			overrides: map[string]any{},
			want:      map[string]*kubemodel.Namespace{},
		},
		{
			name: "basic namespace info and uptime",
			overrides: map[string]any{
				source.QueryNamespaceInfo: []*source.NamespaceInfoResult{
					{UID: "ns-1", Namespace: "default"},
				},
				source.QueryNamespaceUptime: []*source.UptimeResult{
					{UID: "ns-1", First: start, Last: end},
				},
			},
			want: map[string]*kubemodel.Namespace{
				"ns-1": {
					UID:   "ns-1",
					Name:  "default",
					Start: start,
					End:   end,
				},
			},
		},
		{
			name: "namespace without uptime is not registered",
			overrides: map[string]any{
				source.QueryNamespaceInfo: []*source.NamespaceInfoResult{
					{UID: "ns-1", Namespace: "default"},
				},
			},
			want: map[string]*kubemodel.Namespace{},
		},
		{
			name: "namespace labels are attached",
			overrides: map[string]any{
				source.QueryNamespaceInfo: []*source.NamespaceInfoResult{
					{UID: "ns-1", Namespace: "production"},
				},
				source.QueryNamespaceUptime: []*source.UptimeResult{
					{UID: "ns-1", First: start, Last: end},
				},
				source.QueryNamespaceLabels: []*source.NamespaceLabelsResult{
					{UID: "ns-1", Labels: map[string]string{"env": "prod", "team": "platform"}},
				},
			},
			want: map[string]*kubemodel.Namespace{
				"ns-1": {
					UID:    "ns-1",
					Name:   "production",
					Start:  start,
					End:    end,
					Labels: map[string]string{"env": "prod", "team": "platform"},
				},
			},
		},
		{
			name: "namespace annotations are attached",
			overrides: map[string]any{
				source.QueryNamespaceInfo: []*source.NamespaceInfoResult{
					{UID: "ns-1", Namespace: "staging"},
				},
				source.QueryNamespaceUptime: []*source.UptimeResult{
					{UID: "ns-1", First: start, Last: end},
				},
				source.QueryNamespaceAnnotations: []*source.NamespaceAnnotationsResult{
					{UID: "ns-1", Annotations: map[string]string{"owner": "team-a"}},
				},
			},
			want: map[string]*kubemodel.Namespace{
				"ns-1": {
					UID:         "ns-1",
					Name:        "staging",
					Start:       start,
					End:         end,
					Annotations: map[string]string{"owner": "team-a"},
				},
			},
		},
		{
			name: "multiple namespaces are registered",
			overrides: map[string]any{
				source.QueryNamespaceInfo: []*source.NamespaceInfoResult{
					{UID: "ns-1", Namespace: "default"},
					{UID: "ns-2", Namespace: "kube-system"},
				},
				source.QueryNamespaceUptime: []*source.UptimeResult{
					{UID: "ns-1", First: start, Last: end},
					{UID: "ns-2", First: start, Last: end},
				},
			},
			want: map[string]*kubemodel.Namespace{
				"ns-1": {UID: "ns-1", Name: "default", Start: start, End: end},
				"ns-2": {UID: "ns-2", Name: "kube-system", Start: start, End: end},
			},
		},
		{
			name: "uptime for unknown namespace is ignored",
			overrides: map[string]any{
				source.QueryNamespaceInfo: []*source.NamespaceInfoResult{
					{UID: "ns-1", Namespace: "default"},
				},
				source.QueryNamespaceUptime: []*source.UptimeResult{
					{UID: "ns-1", First: start, Last: end},
					{UID: "unknown-ns", First: start, Last: end},
				},
			},
			want: map[string]*kubemodel.Namespace{
				"ns-1": {UID: "ns-1", Name: "default", Start: start, End: end},
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

			assert.Equal(t, tt.want, kms.Namespaces)
		})
	}
}