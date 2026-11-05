package kubemodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
)

func TestComputeDaemonSets(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)

	tests := []struct {
		name      string
		overrides map[string]any
		want      map[string]*kubemodel.DaemonSet
	}{
		{
			name:      "no data returns empty daemonset map",
			overrides: map[string]any{},
			want:      map[string]*kubemodel.DaemonSet{},
		},
		{
			name: "basic daemonset info and uptime",
			overrides: map[string]any{
				source.QueryDaemonSetInfo: []*source.DaemonSetInfoResult{
					{UID: "ds-1", DaemonSet: "fluentd", NamespaceUID: "ns-1"},
				},
				source.QueryDaemonSetUptime: []*source.UptimeResult{
					{UID: "ds-1", First: start, Last: end},
				},
			},
			want: map[string]*kubemodel.DaemonSet{
				"ds-1": {
					UID:          "ds-1",
					Name:         "fluentd",
					NamespaceUID: "ns-1",
					Start:        start,
					End:          end,
				},
			},
		},
		{
			name: "daemonset without uptime is not registered",
			overrides: map[string]any{
				source.QueryDaemonSetInfo: []*source.DaemonSetInfoResult{
					{UID: "ds-1", DaemonSet: "fluentd", NamespaceUID: "ns-1"},
				},
			},
			want: map[string]*kubemodel.DaemonSet{},
		},
		{
			name: "daemonset without namespace uid is not registered",
			overrides: map[string]any{
				source.QueryDaemonSetInfo: []*source.DaemonSetInfoResult{
					{UID: "ds-1", DaemonSet: "fluentd"},
				},
				source.QueryDaemonSetUptime: []*source.UptimeResult{
					{UID: "ds-1", First: start, Last: end},
				},
			},
			want: map[string]*kubemodel.DaemonSet{},
		},
		{
			name: "daemonset labels, annotations, and arguments are attached",
			overrides: map[string]any{
				source.QueryDaemonSetInfo: []*source.DaemonSetInfoResult{
					{UID: "ds-1", DaemonSet: "fluentd", NamespaceUID: "ns-1"},
				},
				source.QueryDaemonSetUptime: []*source.UptimeResult{
					{UID: "ds-1", First: start, Last: end},
				},
				source.QueryDaemonSetLabels: []*source.LabelsResult{
					{UID: "ds-1", Labels: map[string]string{"component": "logging"}},
				},
				source.QueryDaemonSetAnnotations: []*source.AnnotationsResult{
					{UID: "ds-1", Annotations: map[string]string{"managed-by": "helm"}},
				},
				source.QueryDaemonSetArguments: []*source.DaemonSetArgumentResult{
					{UID: "ds-1", Arg: "vgpu", Value: "2"},
					{UID: "ds-1", Arg: "log-level", Value: "debug"},
				},
			},
			want: map[string]*kubemodel.DaemonSet{
				"ds-1": {
					UID:          "ds-1",
					Name:         "fluentd",
					NamespaceUID: "ns-1",
					Start:        start,
					End:          end,
					Labels:       map[string]string{"component": "logging"},
					Annotations:  map[string]string{"managed-by": "helm"},
					Arguments:    map[string]string{"vgpu": "2", "log-level": "debug"},
				},
			},
		},
		{
			name: "arguments for unknown daemonset are ignored",
			overrides: map[string]any{
				source.QueryDaemonSetInfo: []*source.DaemonSetInfoResult{
					{UID: "ds-1", DaemonSet: "fluentd", NamespaceUID: "ns-1"},
				},
				source.QueryDaemonSetUptime: []*source.UptimeResult{
					{UID: "ds-1", First: start, Last: end},
				},
				source.QueryDaemonSetArguments: []*source.DaemonSetArgumentResult{
					{UID: "ds-1", Arg: "vgpu", Value: "2"},
					{UID: "unknown-ds", Arg: "vgpu", Value: "4"},
				},
			},
			want: map[string]*kubemodel.DaemonSet{
				"ds-1": {
					UID:          "ds-1",
					Name:         "fluentd",
					NamespaceUID: "ns-1",
					Start:        start,
					End:          end,
					Arguments:    map[string]string{"vgpu": "2"},
				},
			},
		},
		{
			name: "uptime for unknown daemonset is ignored",
			overrides: map[string]any{
				source.QueryDaemonSetInfo: []*source.DaemonSetInfoResult{
					{UID: "ds-1", DaemonSet: "fluentd", NamespaceUID: "ns-1"},
				},
				source.QueryDaemonSetUptime: []*source.UptimeResult{
					{UID: "ds-1", First: start, Last: end},
					{UID: "unknown-ds", First: start, Last: end},
				},
			},
			want: map[string]*kubemodel.DaemonSet{
				"ds-1": {
					UID:          "ds-1",
					Name:         "fluentd",
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

			assert.Equal(t, tt.want, kms.DaemonSets)
		})
	}
}
