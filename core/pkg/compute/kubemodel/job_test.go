package kubemodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
)

func TestComputeJobs(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)

	tests := []struct {
		name      string
		overrides map[string]any
		want      map[string]*kubemodel.Job
	}{
		{
			name:      "no data returns empty job map",
			overrides: map[string]any{},
			want:      map[string]*kubemodel.Job{},
		},
		{
			name: "basic job info and uptime",
			overrides: map[string]any{
				source.QueryJobInfo: []*source.JobInfoResult{
					{UID: "job-1", Job: "batch-processor", NamespaceUID: "ns-1"},
				},
				source.QueryJobUptime: []*source.UptimeResult{
					{UID: "job-1", First: start, Last: end},
				},
			},
			want: map[string]*kubemodel.Job{
				"job-1": {
					UID:          "job-1",
					Name:         "batch-processor",
					NamespaceUID: "ns-1",
					Start:        start,
					End:          end,
				},
			},
		},
		{
			name: "job without uptime is not registered",
			overrides: map[string]any{
				source.QueryJobInfo: []*source.JobInfoResult{
					{UID: "job-1", Job: "batch-processor", NamespaceUID: "ns-1"},
				},
			},
			want: map[string]*kubemodel.Job{},
		},
		{
			name: "job without namespace uid is not registered",
			overrides: map[string]any{
				source.QueryJobInfo: []*source.JobInfoResult{
					{UID: "job-1", Job: "batch-processor"},
				},
				source.QueryJobUptime: []*source.UptimeResult{
					{UID: "job-1", First: start, Last: end},
				},
			},
			want: map[string]*kubemodel.Job{},
		},
		{
			name: "job labels and annotations are attached",
			overrides: map[string]any{
				source.QueryJobInfo: []*source.JobInfoResult{
					{UID: "job-1", Job: "etl-job", NamespaceUID: "ns-1"},
				},
				source.QueryJobUptime: []*source.UptimeResult{
					{UID: "job-1", First: start, Last: end},
				},
				source.QueryJobLabels: []*source.LabelsResult{
					{UID: "job-1", Labels: map[string]string{"batch": "nightly"}},
				},
				source.QueryJobAnnotations: []*source.AnnotationsResult{
					{UID: "job-1", Annotations: map[string]string{"schedule": "0 2 * * *"}},
				},
			},
			want: map[string]*kubemodel.Job{
				"job-1": {
					UID:          "job-1",
					Name:         "etl-job",
					NamespaceUID: "ns-1",
					Start:        start,
					End:          end,
					Labels:       map[string]string{"batch": "nightly"},
					Annotations:  map[string]string{"schedule": "0 2 * * *"},
				},
			},
		},
		{
			name: "uptime for unknown job is ignored",
			overrides: map[string]any{
				source.QueryJobInfo: []*source.JobInfoResult{
					{UID: "job-1", Job: "batch-processor", NamespaceUID: "ns-1"},
				},
				source.QueryJobUptime: []*source.UptimeResult{
					{UID: "job-1", First: start, Last: end},
					{UID: "unknown-job", First: start, Last: end},
				},
			},
			want: map[string]*kubemodel.Job{
				"job-1": {
					UID:          "job-1",
					Name:         "batch-processor",
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

			assert.Equal(t, tt.want, kms.Jobs)
		})
	}
}
