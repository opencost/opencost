package kubemodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestValidateCluster(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Hour)
	end := start.Add(time.Hour)
	window := Window{Start: start, End: end}

	tests := []struct {
		name    string
		cluster *Cluster
		wantErr string
	}{
		{
			name:    "empty UID",
			cluster: &Cluster{Name: "my-cluster", Start: start, End: end},
			wantErr: "UID is missing for Cluster with name 'my-cluster'",
		},
		{
			name:    "outside window",
			cluster: &Cluster{UID: "cluster-uid", Name: "my-cluster", Start: start.Add(-time.Hour), End: end},
			wantErr: checkWindow(window, start.Add(-time.Hour), end).Error(),
		},
		{
			name:    "valid",
			cluster: &Cluster{UID: "cluster-uid", Name: "my-cluster", Start: start, End: end},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cluster.ValidateCluster(window)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRegisterCluster(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Hour)
	end := start.Add(time.Hour)

	newCluster := func(uid, name string) *Cluster {
		return &Cluster{UID: uid, Name: name, Start: start, End: end}
	}

	tests := []struct {
		name    string
		setup   func(*KubeModelSet)
		cluster *Cluster
		wantErr string
		want    *KubeModelSet
	}{
		{
			name:    "validation failure",
			cluster: &Cluster{UID: "", Name: "my-cluster", Start: start, End: end},
			wantErr: "RegisterCluster: invalid cluster: UID is missing for Cluster with name 'my-cluster'",
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				kms.Metadata.Diagnostics = []Diagnostic{
					{Level: DiagnosticLevelError, Message: "RegisterCluster: invalid cluster: UID is missing for Cluster with name 'my-cluster'"},
				}
				return kms
			}(),
		},
		{
			name:    "registers cluster",
			cluster: newCluster("cluster-uid", "my-cluster"),
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				kms.Cluster = newCluster("cluster-uid", "my-cluster")
				return kms
			}(),
		},
		{
			name: "same UID is a no-op",
			setup: func(kms *KubeModelSet) {
				kms.RegisterCluster(newCluster("cluster-uid", "original"))
			},
			cluster: newCluster("cluster-uid", "duplicate"),
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				kms.Cluster = newCluster("cluster-uid", "original")
				return kms
			}(),
		},
		{
			name: "different UID emits warning and keeps original",
			setup: func(kms *KubeModelSet) {
				kms.RegisterCluster(newCluster("cluster-uid", "original"))
			},
			cluster: newCluster("another-uid", "another"),
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				kms.Cluster = newCluster("cluster-uid", "original")
				kms.Metadata.Diagnostics = []Diagnostic{
					{Level: DiagnosticLevelWarning, Message: "RegisterCluster(another-uid): attempting to change cluster UID from cluster-uid to another-uid"},
				}
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

			err := kms.RegisterCluster(tt.cluster)

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}

			KubeModelSetEquals(t, tt.want, kms)
		})
	}
}
