package kubemodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestValidateJob(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Hour)
	end := start.Add(time.Hour)
	window := Window{Start: start, End: end}

	tests := []struct {
		name    string
		job     *Job
		wantErr string
	}{
		{
			name:    "empty UID",
			job:     &Job{Name: "my-job", NamespaceUID: "ns-uid", Start: start, End: end},
			wantErr: "UID is missing for Job with name 'my-job'",
		},
		{
			name:    "empty Name",
			job:     &Job{UID: "job-uid", NamespaceUID: "ns-uid", Start: start, End: end},
			wantErr: "Name is missing for Job 'job-uid'",
		},
		{
			name:    "empty NamespaceUID",
			job:     &Job{UID: "job-uid", Name: "my-job", Start: start, End: end},
			wantErr: "NamespaceUID is missing for Job 'job-uid'",
		},
		{
			name:    "outside window",
			job:     &Job{UID: "job-uid", Name: "my-job", NamespaceUID: "ns-uid", Start: start.Add(-time.Hour), End: end},
			wantErr: checkWindow(window, start.Add(-time.Hour), end).Error(),
		},
		{
			name: "valid",
			job:  &Job{UID: "job-uid", Name: "my-job", NamespaceUID: "ns-uid", Start: start, End: end},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.job.ValidateJob(window)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRegisterJob(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Hour)
	end := start.Add(time.Hour)

	newJob := func(uid, name string) *Job {
		return &Job{UID: uid, Name: name, NamespaceUID: "ns-uid", Start: start, End: end}
	}
	withCluster := func(kms *KubeModelSet) {
		kms.RegisterCluster(&Cluster{UID: "cluster-uid", Start: start, End: end})
	}

	tests := []struct {
		name    string
		setup   func(*KubeModelSet)
		job     *Job
		wantErr string
		want    *KubeModelSet
	}{
		{
			name:    "validation failure",
			job:     &Job{UID: "", Name: "my-job", NamespaceUID: "ns-uid", Start: start, End: end},
			wantErr: "RegisterJob: invalid job: UID is missing for Job with name 'my-job'",
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				kms.Metadata.Diagnostics = []Diagnostic{
					{Level: DiagnosticLevelError, Message: "RegisterJob: invalid job: UID is missing for Job with name 'my-job'"},
				}
				return kms
			}(),
		},
		{
			name: "warns when cluster is nil",
			job:  newJob("job-uid", "my-job"),
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				kms.Jobs["job-uid"] = newJob("job-uid", "my-job")
				kms.Metadata.ObjectCount = 1
				kms.Metadata.Diagnostics = []Diagnostic{
					{Level: DiagnosticLevelWarning, Message: "RegisterJob: Cluster is nil"},
				}
				return kms
			}(),
		},
		{
			name:  "registers job with cluster",
			setup: withCluster,
			job:   newJob("job-uid", "my-job"),
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				withCluster(kms)
				kms.Jobs["job-uid"] = newJob("job-uid", "my-job")
				kms.Metadata.ObjectCount = 1
				return kms
			}(),
		},
		{
			name: "duplicate registration is a no-op",
			setup: func(kms *KubeModelSet) {
				withCluster(kms)
				kms.RegisterJob(newJob("job-uid", "original"))
			},
			job: newJob("job-uid", "duplicate"),
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				withCluster(kms)
				kms.Jobs["job-uid"] = newJob("job-uid", "original")
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

			err := kms.RegisterJob(tt.job)

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}

			KubeModelSetEquals(t, tt.want, kms)
		})
	}
}
