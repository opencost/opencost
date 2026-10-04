package kubemodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestValidateStatefulSet(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Hour)
	end := start.Add(time.Hour)
	window := Window{Start: start, End: end}

	tests := []struct {
		name        string
		statefulSet *StatefulSet
		wantErr     string
	}{
		{
			name:        "empty UID",
			statefulSet: &StatefulSet{Name: "my-sts", NamespaceUID: "ns-uid", Start: start, End: end},
			wantErr:     "UID is missing for StatefulSet with name 'my-sts'",
		},
		{
			name:        "empty Name",
			statefulSet: &StatefulSet{UID: "sts-uid", NamespaceUID: "ns-uid", Start: start, End: end},
			wantErr:     "Name is missing for StatefulSet 'sts-uid'",
		},
		{
			name:        "empty NamespaceUID",
			statefulSet: &StatefulSet{UID: "sts-uid", Name: "my-sts", Start: start, End: end},
			wantErr:     "NamespaceUID is missing for StatefulSet 'sts-uid'",
		},
		{
			name:        "outside window",
			statefulSet: &StatefulSet{UID: "sts-uid", Name: "my-sts", NamespaceUID: "ns-uid", Start: start.Add(-time.Hour), End: end},
			wantErr:     checkWindow(window, start.Add(-time.Hour), end).Error(),
		},
		{
			name:        "valid",
			statefulSet: &StatefulSet{UID: "sts-uid", Name: "my-sts", NamespaceUID: "ns-uid", Start: start, End: end},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.statefulSet.ValidateStatefulSet(window)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRegisterStatefulSet(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Hour)
	end := start.Add(time.Hour)

	newStatefulSet := func(uid, name string) *StatefulSet {
		return &StatefulSet{UID: uid, Name: name, NamespaceUID: "ns-uid", Start: start, End: end}
	}
	withCluster := func(kms *KubeModelSet) {
		kms.RegisterCluster(&Cluster{UID: "cluster-uid", Start: start, End: end})
	}

	tests := []struct {
		name        string
		setup       func(*KubeModelSet)
		statefulSet *StatefulSet
		wantErr     string
		want        *KubeModelSet
	}{
		{
			name:        "validation failure",
			statefulSet: &StatefulSet{UID: "", Name: "my-sts", NamespaceUID: "ns-uid", Start: start, End: end},
			wantErr:     "RegisterStatefulSet: invalid statefulset: UID is missing for StatefulSet with name 'my-sts'",
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				kms.Metadata.Diagnostics = []Diagnostic{
					{Level: DiagnosticLevelError, Message: "RegisterStatefulSet: invalid statefulset: UID is missing for StatefulSet with name 'my-sts'"},
				}
				return kms
			}(),
		},
		{
			name:        "warns when cluster is nil",
			statefulSet: newStatefulSet("sts-uid", "my-sts"),
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				kms.StatefulSets["sts-uid"] = newStatefulSet("sts-uid", "my-sts")
				kms.Metadata.ObjectCount = 1
				kms.Metadata.Diagnostics = []Diagnostic{
					{Level: DiagnosticLevelWarning, Message: "RegisterStatefulSet: Cluster is nil"},
				}
				return kms
			}(),
		},
		{
			name:        "registers statefulset with cluster",
			setup:       withCluster,
			statefulSet: newStatefulSet("sts-uid", "my-sts"),
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				withCluster(kms)
				kms.StatefulSets["sts-uid"] = newStatefulSet("sts-uid", "my-sts")
				kms.Metadata.ObjectCount = 1
				return kms
			}(),
		},
		{
			name: "duplicate registration is a no-op",
			setup: func(kms *KubeModelSet) {
				withCluster(kms)
				kms.RegisterStatefulSet(newStatefulSet("sts-uid", "original"))
			},
			statefulSet: newStatefulSet("sts-uid", "duplicate"),
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				withCluster(kms)
				kms.StatefulSets["sts-uid"] = newStatefulSet("sts-uid", "original")
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

			err := kms.RegisterStatefulSet(tt.statefulSet)

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}

			KubeModelSetEquals(t, tt.want, kms)
		})
	}
}
