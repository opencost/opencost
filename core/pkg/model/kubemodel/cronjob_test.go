package kubemodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestValidateCronJob(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Hour)
	end := start.Add(time.Hour)
	window := Window{Start: start, End: end}

	tests := []struct {
		name    string
		cronJob *CronJob
		wantErr string
	}{
		{
			name:    "empty UID",
			cronJob: &CronJob{Name: "my-cj", NamespaceUID: "ns-uid", Start: start, End: end},
			wantErr: "UID is missing for CronJob with name 'my-cj'",
		},
		{
			name:    "empty Name",
			cronJob: &CronJob{UID: "cj-uid", NamespaceUID: "ns-uid", Start: start, End: end},
			wantErr: "Name is missing for CronJob 'cj-uid'",
		},
		{
			name:    "empty NamespaceUID",
			cronJob: &CronJob{UID: "cj-uid", Name: "my-cj", Start: start, End: end},
			wantErr: "NamespaceUID is missing for CronJob 'cj-uid'",
		},
		{
			name:    "outside window",
			cronJob: &CronJob{UID: "cj-uid", Name: "my-cj", NamespaceUID: "ns-uid", Start: start.Add(-time.Hour), End: end},
			wantErr: checkWindow(window, start.Add(-time.Hour), end).Error(),
		},
		{
			name:    "valid",
			cronJob: &CronJob{UID: "cj-uid", Name: "my-cj", NamespaceUID: "ns-uid", Start: start, End: end},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cronJob.ValidateCronJob(window)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRegisterCronJob(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Hour)
	end := start.Add(time.Hour)

	newCronJob := func(uid, name string) *CronJob {
		return &CronJob{UID: uid, Name: name, NamespaceUID: "ns-uid", Start: start, End: end}
	}
	withCluster := func(kms *KubeModelSet) {
		kms.RegisterCluster(&Cluster{UID: "cluster-uid", Start: start, End: end})
	}

	tests := []struct {
		name    string
		setup   func(*KubeModelSet)
		cronJob *CronJob
		wantErr string
		want    *KubeModelSet
	}{
		{
			name:    "validation failure",
			cronJob: &CronJob{UID: "", Name: "my-cj", NamespaceUID: "ns-uid", Start: start, End: end},
			wantErr: "RegisterCronJob: invalid cronjob: UID is missing for CronJob with name 'my-cj'",
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				kms.Metadata.Diagnostics = []Diagnostic{
					{Level: DiagnosticLevelError, Message: "RegisterCronJob: invalid cronjob: UID is missing for CronJob with name 'my-cj'"},
				}
				return kms
			}(),
		},
		{
			name:    "warns when cluster is nil",
			cronJob: newCronJob("cj-uid", "my-cj"),
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				kms.CronJobs["cj-uid"] = newCronJob("cj-uid", "my-cj")
				kms.Metadata.ObjectCount = 1
				kms.Metadata.Diagnostics = []Diagnostic{
					{Level: DiagnosticLevelWarning, Message: "RegisterCronJob: Cluster is nil"},
				}
				return kms
			}(),
		},
		{
			name:    "registers cronjob with cluster",
			setup:   withCluster,
			cronJob: newCronJob("cj-uid", "my-cj"),
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				withCluster(kms)
				kms.CronJobs["cj-uid"] = newCronJob("cj-uid", "my-cj")
				kms.Metadata.ObjectCount = 1
				return kms
			}(),
		},
		{
			name: "duplicate registration is a no-op",
			setup: func(kms *KubeModelSet) {
				withCluster(kms)
				kms.RegisterCronJob(newCronJob("cj-uid", "original"))
			},
			cronJob: newCronJob("cj-uid", "duplicate"),
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				withCluster(kms)
				kms.CronJobs["cj-uid"] = newCronJob("cj-uid", "original")
				kms.Metadata.ObjectCount = 1
				return kms
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kms := NewKubeModelSet(start, end)
			if tt.setup != nil {
				tt.setup(kms)
			}

			err := kms.RegisterCronJob(tt.cronJob)

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}

			KubeModelSetEquals(t, tt.want, kms)
		})
	}
}
