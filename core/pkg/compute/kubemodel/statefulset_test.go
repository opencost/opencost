package kubemodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
)

func TestComputeStatefulSets(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)

	tests := []struct {
		name      string
		overrides map[string]any
		want      map[string]*kubemodel.StatefulSet
	}{
		{
			name:      "no data returns empty statefulset map",
			overrides: map[string]any{},
			want:      map[string]*kubemodel.StatefulSet{},
		},
		{
			name: "basic statefulset info and uptime",
			overrides: map[string]any{
				source.QueryStatefulSetInfo: []*source.StatefulSetInfoResult{
					{UID: "sts-1", StatefulSet: "my-db", NamespaceUID: "ns-1"},
				},
				source.QueryStatefulSetUptime: []*source.UptimeResult{
					{UID: "sts-1", First: start, Last: end},
				},
			},
			want: map[string]*kubemodel.StatefulSet{
				"sts-1": {
					UID:          "sts-1",
					Name:         "my-db",
					NamespaceUID: "ns-1",
					Start:        start,
					End:          end,
				},
			},
		},
		{
			name: "statefulset without uptime is not registered",
			overrides: map[string]any{
				source.QueryStatefulSetInfo: []*source.StatefulSetInfoResult{
					{UID: "sts-1", StatefulSet: "my-db", NamespaceUID: "ns-1"},
				},
			},
			want: map[string]*kubemodel.StatefulSet{},
		},
		{
			name: "statefulset without namespace uid is not registered",
			overrides: map[string]any{
				source.QueryStatefulSetInfo: []*source.StatefulSetInfoResult{
					{UID: "sts-1", StatefulSet: "my-db"},
				},
				source.QueryStatefulSetUptime: []*source.UptimeResult{
					{UID: "sts-1", First: start, Last: end},
				},
			},
			want: map[string]*kubemodel.StatefulSet{},
		},
		{
			name: "statefulset labels and annotations are attached",
			overrides: map[string]any{
				source.QueryStatefulSetInfo: []*source.StatefulSetInfoResult{
					{UID: "sts-1", StatefulSet: "my-db", NamespaceUID: "ns-1"},
				},
				source.QueryStatefulSetUptime: []*source.UptimeResult{
					{UID: "sts-1", First: start, Last: end},
				},
				source.QueryStatefulSetLabels: []*source.LabelsResult{
					{UID: "sts-1", Labels: map[string]string{"app": "postgres"}},
				},
				source.QueryStatefulSetAnnotations: []*source.AnnotationsResult{
					{UID: "sts-1", Annotations: map[string]string{"version": "14"}},
				},
			},
			want: map[string]*kubemodel.StatefulSet{
				"sts-1": {
					UID:          "sts-1",
					Name:         "my-db",
					NamespaceUID: "ns-1",
					Start:        start,
					End:          end,
					Labels:       map[string]string{"app": "postgres"},
					Annotations:  map[string]string{"version": "14"},
				},
			},
		},
		{
			name: "statefulset match labels are attached",
			overrides: map[string]any{
				source.QueryStatefulSetInfo: []*source.StatefulSetInfoResult{
					{UID: "sts-1", StatefulSet: "my-db", NamespaceUID: "ns-1"},
				},
				source.QueryStatefulSetUptime: []*source.UptimeResult{
					{UID: "sts-1", First: start, Last: end},
				},
				source.QueryStatefulSetMatchLabels: []*source.StatefulSetLabelsResult{
					{UID: "sts-1", Labels: map[string]string{"app": "postgres"}},
				},
			},
			want: map[string]*kubemodel.StatefulSet{
				"sts-1": {
					UID:          "sts-1",
					Name:         "my-db",
					NamespaceUID: "ns-1",
					Start:        start,
					End:          end,
					MatchLabels:  map[string]string{"app": "postgres"},
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

			assert.Equal(t, tt.want, kms.StatefulSets)
		})
	}
}
