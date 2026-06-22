package kubemodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestValidateDaemonSet(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Hour)
	end := start.Add(time.Hour)
	window := Window{Start: start, End: end}

	tests := []struct {
		name      string
		daemonSet *DaemonSet
		wantErr   string
	}{
		{
			name:      "empty UID",
			daemonSet: &DaemonSet{Name: "my-ds", NamespaceUID: "ns-uid", Start: start, End: end},
			wantErr:   "UID is missing for DaemonSet with name 'my-ds'",
		},
		{
			name:      "empty Name",
			daemonSet: &DaemonSet{UID: "ds-uid", NamespaceUID: "ns-uid", Start: start, End: end},
			wantErr:   "Name is missing for DaemonSet 'ds-uid'",
		},
		{
			name:      "empty NamespaceUID",
			daemonSet: &DaemonSet{UID: "ds-uid", Name: "my-ds", Start: start, End: end},
			wantErr:   "NamespaceUID is missing for DaemonSet 'ds-uid'",
		},
		{
			name:      "outside window",
			daemonSet: &DaemonSet{UID: "ds-uid", Name: "my-ds", NamespaceUID: "ns-uid", Start: start.Add(-time.Hour), End: end},
			wantErr:   checkWindow(window, start.Add(-time.Hour), end).Error(),
		},
		{
			name:      "valid",
			daemonSet: &DaemonSet{UID: "ds-uid", Name: "my-ds", NamespaceUID: "ns-uid", Start: start, End: end},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.daemonSet.ValidateDaemonSet(window)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRegisterDaemonSet(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Hour)
	end := start.Add(time.Hour)

	newDaemonSet := func(uid, name string) *DaemonSet {
		return &DaemonSet{UID: uid, Name: name, NamespaceUID: "ns-uid", Start: start, End: end}
	}
	withCluster := func(kms *KubeModelSet) {
		kms.RegisterCluster(&Cluster{UID: "cluster-uid", Start: start, End: end})
	}

	tests := []struct {
		name      string
		setup     func(*KubeModelSet)
		daemonSet *DaemonSet
		wantErr   string
		want      *KubeModelSet
	}{
		{
			name:      "validation failure",
			daemonSet: &DaemonSet{UID: "", Name: "my-ds", NamespaceUID: "ns-uid", Start: start, End: end},
			wantErr:   "RegisterDaemonSet: invalid daemonset: UID is missing for DaemonSet with name 'my-ds'",
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				kms.Metadata.Diagnostics = []Diagnostic{
					{Level: DiagnosticLevelError, Message: "RegisterDaemonSet: invalid daemonset: UID is missing for DaemonSet with name 'my-ds'"},
				}
				return kms
			}(),
		},
		{
			name:      "warns when cluster is nil",
			daemonSet: newDaemonSet("ds-uid", "my-ds"),
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				kms.DaemonSets["ds-uid"] = newDaemonSet("ds-uid", "my-ds")
				kms.Metadata.ObjectCount = 1
				kms.Metadata.Diagnostics = []Diagnostic{
					{Level: DiagnosticLevelWarning, Message: "RegisterDaemonSet: Cluster is nil"},
				}
				return kms
			}(),
		},
		{
			name:      "registers daemonset with cluster",
			setup:     withCluster,
			daemonSet: newDaemonSet("ds-uid", "my-ds"),
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				withCluster(kms)
				kms.DaemonSets["ds-uid"] = newDaemonSet("ds-uid", "my-ds")
				kms.Metadata.ObjectCount = 1
				return kms
			}(),
		},
		{
			name: "duplicate registration is a no-op",
			setup: func(kms *KubeModelSet) {
				withCluster(kms)
				kms.RegisterDaemonSet(newDaemonSet("ds-uid", "original"))
			},
			daemonSet: newDaemonSet("ds-uid", "duplicate"),
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				withCluster(kms)
				kms.DaemonSets["ds-uid"] = newDaemonSet("ds-uid", "original")
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

			err := kms.RegisterDaemonSet(tt.daemonSet)

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}

			KubeModelSetEquals(t, tt.want, kms)
		})
	}
}
