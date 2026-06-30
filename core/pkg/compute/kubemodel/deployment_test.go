package kubemodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
)

func TestComputeDeployments(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)

	tests := []struct {
		name      string
		overrides map[string]any
		want      map[string]*kubemodel.Deployment
	}{
		{
			name:      "no data returns empty deployment map",
			overrides: map[string]any{},
			want:      map[string]*kubemodel.Deployment{},
		},
		{
			name: "basic deployment info and uptime",
			overrides: map[string]any{
				source.QueryDeploymentInfo: []*source.DeploymentInfoResult{
					{UID: "dep-1", Deployment: "my-app", NamespaceUID: "ns-1"},
				},
				source.QueryDeploymentUptime: []*source.UptimeResult{
					{UID: "dep-1", First: start, Last: end},
				},
			},
			want: map[string]*kubemodel.Deployment{
				"dep-1": {
					UID:          "dep-1",
					Name:         "my-app",
					NamespaceUID: "ns-1",
					Start:        start,
					End:          end,
				},
			},
		},
		{
			name: "deployment without uptime is not registered",
			overrides: map[string]any{
				source.QueryDeploymentInfo: []*source.DeploymentInfoResult{
					{UID: "dep-1", Deployment: "my-app", NamespaceUID: "ns-1"},
				},
			},
			want: map[string]*kubemodel.Deployment{},
		},
		{
			name: "deployment without namespace uid is not registered",
			overrides: map[string]any{
				source.QueryDeploymentInfo: []*source.DeploymentInfoResult{
					{UID: "dep-1", Deployment: "my-app"},
				},
				source.QueryDeploymentUptime: []*source.UptimeResult{
					{UID: "dep-1", First: start, Last: end},
				},
			},
			want: map[string]*kubemodel.Deployment{},
		},
		{
			name: "deployment labels and annotations are attached",
			overrides: map[string]any{
				source.QueryDeploymentInfo: []*source.DeploymentInfoResult{
					{UID: "dep-1", Deployment: "my-app", NamespaceUID: "ns-1"},
				},
				source.QueryDeploymentUptime: []*source.UptimeResult{
					{UID: "dep-1", First: start, Last: end},
				},
				source.QueryDeploymentLabels: []*source.LabelsResult{
					{UID: "dep-1", Labels: map[string]string{"app": "web"}},
				},
				source.QueryDeploymentAnnotations: []*source.AnnotationsResult{
					{UID: "dep-1", Annotations: map[string]string{"team": "platform"}},
				},
			},
			want: map[string]*kubemodel.Deployment{
				"dep-1": {
					UID:          "dep-1",
					Name:         "my-app",
					NamespaceUID: "ns-1",
					Start:        start,
					End:          end,
					Labels:       map[string]string{"app": "web"},
					Annotations:  map[string]string{"team": "platform"},
				},
			},
		},
		{
			name: "deployment match labels are attached",
			overrides: map[string]any{
				source.QueryDeploymentInfo: []*source.DeploymentInfoResult{
					{UID: "dep-1", Deployment: "my-app", NamespaceUID: "ns-1"},
				},
				source.QueryDeploymentUptime: []*source.UptimeResult{
					{UID: "dep-1", First: start, Last: end},
				},
				source.QueryDeploymentMatchLabels: []*source.DeploymentLabelsResult{
					{UID: "dep-1", Labels: map[string]string{"app": "web", "tier": "frontend"}},
				},
			},
			want: map[string]*kubemodel.Deployment{
				"dep-1": {
					UID:          "dep-1",
					Name:         "my-app",
					NamespaceUID: "ns-1",
					Start:        start,
					End:          end,
					MatchLabels:  map[string]string{"app": "web", "tier": "frontend"},
				},
			},
		},
		{
			name: "uptime for unknown deployment is ignored",
			overrides: map[string]any{
				source.QueryDeploymentInfo: []*source.DeploymentInfoResult{
					{UID: "dep-1", Deployment: "my-app", NamespaceUID: "ns-1"},
				},
				source.QueryDeploymentUptime: []*source.UptimeResult{
					{UID: "dep-1", First: start, Last: end},
					{UID: "unknown-dep", First: start, Last: end},
				},
			},
			want: map[string]*kubemodel.Deployment{
				"dep-1": {
					UID:          "dep-1",
					Name:         "my-app",
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

			km, err := NewKubeModel(testClusterUID, ds)
			require.NoError(t, err)

			kms, err := km.ComputeKubeModelSet(start, end)
			require.NoError(t, err)

			assert.Equal(t, tt.want, kms.Deployments)
		})
	}
}
