package kubemodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
)

func TestComputeCronJobs(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)

	tests := []struct {
		name      string
		overrides map[string]any
		want      map[string]*kubemodel.CronJob
	}{
		{
			name:      "no data returns empty cronjob map",
			overrides: map[string]any{},
			want:      map[string]*kubemodel.CronJob{},
		},
		{
			name: "basic cronjob info and uptime",
			overrides: map[string]any{
				source.QueryCronJobInfo: []*source.CronJobInfoResult{
					{UID: "cj-1", CronJob: "nightly-backup", NamespaceUID: "ns-1"},
				},
				source.QueryCronJobUptime: []*source.UptimeResult{
					{UID: "cj-1", First: start, Last: end},
				},
			},
			want: map[string]*kubemodel.CronJob{
				"cj-1": {
					UID:          "cj-1",
					Name:         "nightly-backup",
					NamespaceUID: "ns-1",
					Start:        start,
					End:          end,
				},
			},
		},
		{
			name: "cronjob without uptime is not registered",
			overrides: map[string]any{
				source.QueryCronJobInfo: []*source.CronJobInfoResult{
					{UID: "cj-1", CronJob: "nightly-backup", NamespaceUID: "ns-1"},
				},
			},
			want: map[string]*kubemodel.CronJob{},
		},
		{
			name: "cronjob without namespace uid is not registered",
			overrides: map[string]any{
				source.QueryCronJobInfo: []*source.CronJobInfoResult{
					{UID: "cj-1", CronJob: "nightly-backup"},
				},
				source.QueryCronJobUptime: []*source.UptimeResult{
					{UID: "cj-1", First: start, Last: end},
				},
			},
			want: map[string]*kubemodel.CronJob{},
		},
		{
			name: "cronjob labels and annotations are attached",
			overrides: map[string]any{
				source.QueryCronJobInfo: []*source.CronJobInfoResult{
					{UID: "cj-1", CronJob: "nightly-backup", NamespaceUID: "ns-1"},
				},
				source.QueryCronJobUptime: []*source.UptimeResult{
					{UID: "cj-1", First: start, Last: end},
				},
				source.QueryCronJobLabels: []*source.LabelsResult{
					{UID: "cj-1", Labels: map[string]string{"schedule": "nightly"}},
				},
				source.QueryCronJobAnnotations: []*source.AnnotationsResult{
					{UID: "cj-1", Annotations: map[string]string{"owner": "ops"}},
				},
			},
			want: map[string]*kubemodel.CronJob{
				"cj-1": {
					UID:          "cj-1",
					Name:         "nightly-backup",
					NamespaceUID: "ns-1",
					Start:        start,
					End:          end,
					Labels:       map[string]string{"schedule": "nightly"},
					Annotations:  map[string]string{"owner": "ops"},
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

			assert.Equal(t, tt.want, kms.CronJobs)
		})
	}
}
