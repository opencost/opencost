package kubemodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestValidateDeployment(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Hour)
	end := start.Add(time.Hour)
	window := Window{Start: start, End: end}

	tests := []struct {
		name       string
		deployment *Deployment
		wantErr    string
	}{
		{
			name:       "empty UID",
			deployment: &Deployment{Name: "my-deployment", NamespaceUID: "ns-uid", Start: start, End: end},
			wantErr:    "UID is missing for Deployment with name 'my-deployment'",
		},
		{
			name:       "empty Name",
			deployment: &Deployment{UID: "dep-uid", NamespaceUID: "ns-uid", Start: start, End: end},
			wantErr:    "Name is missing for Deployment 'dep-uid'",
		},
		{
			name:       "empty NamespaceUID",
			deployment: &Deployment{UID: "dep-uid", Name: "my-deployment", Start: start, End: end},
			wantErr:    "NamespaceUID is missing for Deployment 'dep-uid'",
		},
		{
			name:       "outside window",
			deployment: &Deployment{UID: "dep-uid", Name: "my-deployment", NamespaceUID: "ns-uid", Start: start.Add(-time.Hour), End: end},
			wantErr:    checkWindow(window, start.Add(-time.Hour), end).Error(),
		},
		{
			name:       "valid",
			deployment: &Deployment{UID: "dep-uid", Name: "my-deployment", NamespaceUID: "ns-uid", Start: start, End: end},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.deployment.ValidateDeployment(window)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRegisterDeployment(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Hour)
	end := start.Add(time.Hour)

	newDeployment := func(uid, name string) *Deployment {
		return &Deployment{UID: uid, Name: name, NamespaceUID: "ns-uid", Start: start, End: end}
	}
	withCluster := func(kms *KubeModelSet) {
		kms.RegisterCluster(&Cluster{UID: "cluster-uid", Start: start, End: end})
	}

	tests := []struct {
		name       string
		setup      func(*KubeModelSet)
		deployment *Deployment
		wantErr    string
		want       *KubeModelSet
	}{
		{
			name:       "validation failure",
			deployment: &Deployment{UID: "", Name: "my-deployment", NamespaceUID: "ns-uid", Start: start, End: end},
			wantErr:    "RegisterDeployment: invalid deployment: UID is missing for Deployment with name 'my-deployment'",
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				kms.Metadata.Diagnostics = []Diagnostic{
					{Level: DiagnosticLevelError, Message: "RegisterDeployment: invalid deployment: UID is missing for Deployment with name 'my-deployment'"},
				}
				return kms
			}(),
		},
		{
			name:       "warns when cluster is nil",
			deployment: newDeployment("dep-uid", "my-deployment"),
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				kms.Deployments["dep-uid"] = newDeployment("dep-uid", "my-deployment")
				kms.Metadata.ObjectCount = 1
				kms.Metadata.Diagnostics = []Diagnostic{
					{Level: DiagnosticLevelWarning, Message: "RegisterDeployment: Cluster is nil"},
				}
				return kms
			}(),
		},
		{
			name:       "registers deployment with cluster",
			setup:      withCluster,
			deployment: newDeployment("dep-uid", "my-deployment"),
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				withCluster(kms)
				kms.Deployments["dep-uid"] = newDeployment("dep-uid", "my-deployment")
				kms.Metadata.ObjectCount = 1
				return kms
			}(),
		},
		{
			name: "duplicate registration is a no-op",
			setup: func(kms *KubeModelSet) {
				withCluster(kms)
				kms.RegisterDeployment(newDeployment("dep-uid", "original"))
			},
			deployment: newDeployment("dep-uid", "duplicate"),
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				withCluster(kms)
				kms.Deployments["dep-uid"] = newDeployment("dep-uid", "original")
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

			err := kms.RegisterDeployment(tt.deployment)

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}

			KubeModelSetEquals(t, tt.want, kms)
		})
	}
}
