package kubemodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestValidateReplicaSet(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Hour)
	end := start.Add(time.Hour)
	window := Window{Start: start, End: end}

	tests := []struct {
		name       string
		replicaSet *ReplicaSet
		wantErr    string
	}{
		{
			name:       "empty UID",
			replicaSet: &ReplicaSet{Name: "my-rs", NamespaceUID: "ns-uid", Start: start, End: end},
			wantErr:    "UID is missing for ReplicaSet with name 'my-rs'",
		},
		{
			name:       "empty Name",
			replicaSet: &ReplicaSet{UID: "rs-uid", NamespaceUID: "ns-uid", Start: start, End: end},
			wantErr:    "Name is missing for ReplicaSet 'rs-uid'",
		},
		{
			name:       "empty NamespaceUID",
			replicaSet: &ReplicaSet{UID: "rs-uid", Name: "my-rs", Start: start, End: end},
			wantErr:    "NamespaceUID is missing for ReplicaSet 'rs-uid'",
		},
		{
			name:       "outside window",
			replicaSet: &ReplicaSet{UID: "rs-uid", Name: "my-rs", NamespaceUID: "ns-uid", Start: start.Add(-time.Hour), End: end},
			wantErr:    checkWindow(window, start.Add(-time.Hour), end).Error(),
		},
		{
			name:       "valid",
			replicaSet: &ReplicaSet{UID: "rs-uid", Name: "my-rs", NamespaceUID: "ns-uid", Start: start, End: end},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.replicaSet.ValidateReplicaSet(window)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRegisterReplicaSet(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Hour)
	end := start.Add(time.Hour)

	newReplicaSet := func(uid, name string) *ReplicaSet {
		return &ReplicaSet{UID: uid, Name: name, NamespaceUID: "ns-uid", Start: start, End: end}
	}
	withCluster := func(kms *KubeModelSet) {
		kms.RegisterCluster(&Cluster{UID: "cluster-uid", Start: start, End: end})
	}

	tests := []struct {
		name       string
		setup      func(*KubeModelSet)
		replicaSet *ReplicaSet
		wantErr    string
		want       *KubeModelSet
	}{
		{
			name:       "validation failure",
			replicaSet: &ReplicaSet{UID: "", Name: "my-rs", NamespaceUID: "ns-uid", Start: start, End: end},
			wantErr:    "RegisterReplicaSet: invalid replicaset: UID is missing for ReplicaSet with name 'my-rs'",
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				kms.Metadata.Diagnostics = []Diagnostic{
					{Level: DiagnosticLevelError, Message: "RegisterReplicaSet: invalid replicaset: UID is missing for ReplicaSet with name 'my-rs'"},
				}
				return kms
			}(),
		},
		{
			name:       "warns when cluster is nil",
			replicaSet: newReplicaSet("rs-uid", "my-rs"),
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				kms.ReplicaSets["rs-uid"] = newReplicaSet("rs-uid", "my-rs")
				kms.Metadata.ObjectCount = 1
				kms.Metadata.Diagnostics = []Diagnostic{
					{Level: DiagnosticLevelWarning, Message: "RegisterReplicaSet: Cluster is nil"},
				}
				return kms
			}(),
		},
		{
			name:       "registers replicaset with cluster",
			setup:      withCluster,
			replicaSet: newReplicaSet("rs-uid", "my-rs"),
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				withCluster(kms)
				kms.ReplicaSets["rs-uid"] = newReplicaSet("rs-uid", "my-rs")
				kms.Metadata.ObjectCount = 1
				return kms
			}(),
		},
		{
			name: "duplicate registration is a no-op",
			setup: func(kms *KubeModelSet) {
				withCluster(kms)
				kms.RegisterReplicaSet(newReplicaSet("rs-uid", "original"))
			},
			replicaSet: newReplicaSet("rs-uid", "duplicate"),
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				withCluster(kms)
				kms.ReplicaSets["rs-uid"] = newReplicaSet("rs-uid", "original")
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

			err := kms.RegisterReplicaSet(tt.replicaSet)

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}

			KubeModelSetEquals(t, tt.want, kms)
		})
	}
}
