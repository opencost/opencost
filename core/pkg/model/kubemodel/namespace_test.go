package kubemodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestValidateNamespace(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Hour)
	end := start.Add(time.Hour)
	window := Window{Start: start, End: end}

	tests := []struct {
		name      string
		namespace *Namespace
		wantErr   string
	}{
		{
			name:      "empty UID",
			namespace: &Namespace{Name: "my-ns", Start: start, End: end},
			wantErr:   "UID is missing for Namespace with name 'my-ns'",
		},
		{
			name:      "empty Name",
			namespace: &Namespace{UID: "ns-uid", Start: start, End: end},
			wantErr:   "Name is missing for Namespace 'ns-uid'",
		},
		{
			name:      "outside window",
			namespace: &Namespace{UID: "ns-uid", Name: "my-ns", Start: start.Add(-time.Hour), End: end},
			wantErr:   checkWindow(window, start.Add(-time.Hour), end).Error(),
		},
		{
			name:      "valid",
			namespace: &Namespace{UID: "ns-uid", Name: "my-ns", Start: start, End: end},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.namespace.ValidateNamespace(window)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRegisterNamespace(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Hour)
	end := start.Add(time.Hour)

	newNamespace := func(uid, name string) *Namespace {
		return &Namespace{UID: uid, Name: name, Start: start, End: end}
	}
	withCluster := func(kms *KubeModelSet) {
		kms.RegisterCluster(&Cluster{UID: "cluster-uid", Start: start, End: end})
	}

	tests := []struct {
		name      string
		setup     func(*KubeModelSet)
		namespace *Namespace
		wantErr   string
		want      *KubeModelSet
	}{
		{
			name:      "validation failure",
			namespace: &Namespace{UID: "", Name: "my-ns", Start: start, End: end},
			wantErr:   "RegisterNamespace: invalid namespace: UID is missing for Namespace with name 'my-ns'",
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				kms.Metadata.Diagnostics = []Diagnostic{
					{Level: DiagnosticLevelError, Message: "RegisterNamespace: invalid namespace: UID is missing for Namespace with name 'my-ns'"},
				}
				return kms
			}(),
		},
		{
			name:      "warns when cluster is nil",
			namespace: newNamespace("ns-uid", "my-ns"),
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				kms.Namespaces["ns-uid"] = newNamespace("ns-uid", "my-ns")
				kms.Metadata.ObjectCount = 1
				kms.Metadata.Diagnostics = []Diagnostic{
					{Level: DiagnosticLevelWarning, Message: "RegisterNamespace: Cluster is nil"},
				}
				return kms
			}(),
		},
		{
			name:      "registers namespace with cluster",
			setup:     withCluster,
			namespace: newNamespace("ns-uid", "my-ns"),
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				withCluster(kms)
				kms.Namespaces["ns-uid"] = newNamespace("ns-uid", "my-ns")
				kms.Metadata.ObjectCount = 1
				return kms
			}(),
		},
		{
			name: "duplicate registration is a no-op",
			setup: func(kms *KubeModelSet) {
				withCluster(kms)
				kms.RegisterNamespace(newNamespace("ns-uid", "original"))
			},
			namespace: newNamespace("ns-uid", "duplicate"),
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				withCluster(kms)
				kms.Namespaces["ns-uid"] = newNamespace("ns-uid", "original")
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

			err := kms.RegisterNamespace(tt.namespace)

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}

			KubeModelSetEquals(t, tt.want, kms)
		})
	}
}
