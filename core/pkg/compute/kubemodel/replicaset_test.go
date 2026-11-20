package kubemodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
)

func TestComputeReplicaSets(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)

	tests := []struct {
		name      string
		overrides map[string]any
		want      map[string]*kubemodel.ReplicaSet
	}{
		{
			name:      "no data returns empty replicaset map",
			overrides: map[string]any{},
			want:      map[string]*kubemodel.ReplicaSet{},
		},
		{
			name: "basic replicaset info and uptime",
			overrides: map[string]any{
				source.QueryReplicaSetInfo: []*source.ReplicaSetInfoResult{
					{UID: "rs-1", ReplicaSet: "my-app-v1", NamespaceUID: "ns-1"},
				},
				source.QueryReplicaSetUptime: []*source.UptimeResult{
					{UID: "rs-1", First: start, Last: end},
				},
			},
			want: map[string]*kubemodel.ReplicaSet{
				"rs-1": {
					UID:          "rs-1",
					Name:         "my-app-v1",
					NamespaceUID: "ns-1",
					Start:        start,
					End:          end,
				},
			},
		},
		{
			name: "replicaset without uptime is not registered",
			overrides: map[string]any{
				source.QueryReplicaSetInfo: []*source.ReplicaSetInfoResult{
					{UID: "rs-1", ReplicaSet: "my-app-v1", NamespaceUID: "ns-1"},
				},
			},
			want: map[string]*kubemodel.ReplicaSet{},
		},
		{
			name: "replicaset without namespace uid is not registered",
			overrides: map[string]any{
				source.QueryReplicaSetInfo: []*source.ReplicaSetInfoResult{
					{UID: "rs-1", ReplicaSet: "my-app-v1"},
				},
				source.QueryReplicaSetUptime: []*source.UptimeResult{
					{UID: "rs-1", First: start, Last: end},
				},
			},
			want: map[string]*kubemodel.ReplicaSet{},
		},
		{
			name: "replicaset owner is attached",
			overrides: map[string]any{
				source.QueryReplicaSetInfo: []*source.ReplicaSetInfoResult{
					{UID: "rs-1", ReplicaSet: "my-app-v1", NamespaceUID: "ns-1"},
				},
				source.QueryReplicaSetUptime: []*source.UptimeResult{
					{UID: "rs-1", First: start, Last: end},
				},
				source.QueryReplicaSetOwners: []*source.OwnerResult{
					{UID: "rs-1", OwnerUID: "dep-1", OwnerKind: "Deployment", Controller: true},
				},
			},
			want: map[string]*kubemodel.ReplicaSet{
				"rs-1": {
					UID:          "rs-1",
					Name:         "my-app-v1",
					NamespaceUID: "ns-1",
					Start:        start,
					End:          end,
					Owners: []kubemodel.Owner{
						{UID: "dep-1", Kind: kubemodel.OwnerKindDeployment, Controller: true},
					},
				},
			},
		},
		{
			name: "replicaset labels and annotations are attached",
			overrides: map[string]any{
				source.QueryReplicaSetInfo: []*source.ReplicaSetInfoResult{
					{UID: "rs-1", ReplicaSet: "my-app-v1", NamespaceUID: "ns-1"},
				},
				source.QueryReplicaSetUptime: []*source.UptimeResult{
					{UID: "rs-1", First: start, Last: end},
				},
				source.QueryReplicaSetLabels: []*source.LabelsResult{
					{UID: "rs-1", Labels: map[string]string{"app": "my-app", "version": "v1"}},
				},
				source.QueryReplicaSetAnnotations: []*source.AnnotationsResult{
					{UID: "rs-1", Annotations: map[string]string{"rollout": "stable"}},
				},
			},
			want: map[string]*kubemodel.ReplicaSet{
				"rs-1": {
					UID:          "rs-1",
					Name:         "my-app-v1",
					NamespaceUID: "ns-1",
					Start:        start,
					End:          end,
					Labels:       map[string]string{"app": "my-app", "version": "v1"},
					Annotations:  map[string]string{"rollout": "stable"},
				},
			},
		},
		{
			name: "owner for unknown replicaset is ignored",
			overrides: map[string]any{
				source.QueryReplicaSetInfo: []*source.ReplicaSetInfoResult{
					{UID: "rs-1", ReplicaSet: "my-app-v1", NamespaceUID: "ns-1"},
				},
				source.QueryReplicaSetUptime: []*source.UptimeResult{
					{UID: "rs-1", First: start, Last: end},
				},
				source.QueryReplicaSetOwners: []*source.OwnerResult{
					{UID: "unknown-rs", OwnerUID: "dep-1", OwnerKind: "Deployment", Controller: true},
				},
			},
			want: map[string]*kubemodel.ReplicaSet{
				"rs-1": {
					UID:          "rs-1",
					Name:         "my-app-v1",
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

			assert.Equal(t, tt.want, kms.ReplicaSets)
		})
	}
}
