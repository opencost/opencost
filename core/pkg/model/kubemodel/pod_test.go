package kubemodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestValidatePod(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Hour)
	end := start.Add(time.Hour)
	window := Window{Start: start, End: end}

	tests := []struct {
		name    string
		pod     *Pod
		wantErr string
	}{
		{
			name:    "empty UID",
			pod:     &Pod{Name: "my-pod", NamespaceUID: "ns-uid", Start: start, End: end},
			wantErr: "UID is missing for Pod with name 'my-pod'",
		},
		{
			name:    "empty Name",
			pod:     &Pod{UID: "pod-uid", NamespaceUID: "ns-uid", Start: start, End: end},
			wantErr: "Name is missing for Pod 'pod-uid'",
		},
		{
			name:    "empty NamespaceUID",
			pod:     &Pod{UID: "pod-uid", Name: "my-pod", Start: start, End: end},
			wantErr: "NamespaceUID is missing for Pod 'pod-uid'",
		},
		{
			name:    "outside window",
			pod:     &Pod{UID: "pod-uid", Name: "my-pod", NamespaceUID: "ns-uid", Start: start.Add(-time.Hour), End: end},
			wantErr: checkWindow(window, start.Add(-time.Hour), end).Error(),
		},
		{
			name: "valid",
			pod:  &Pod{UID: "pod-uid", Name: "my-pod", NamespaceUID: "ns-uid", Start: start, End: end},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.pod.ValidatePod(window)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRegisterPod(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Hour)
	end := start.Add(time.Hour)

	newPod := func(uid, name string) *Pod {
		return &Pod{UID: uid, Name: name, NamespaceUID: "ns-uid", Start: start, End: end}
	}
	withCluster := func(kms *KubeModelSet) {
		kms.RegisterCluster(&Cluster{UID: "cluster-uid", Start: start, End: end})
	}

	tests := []struct {
		name    string
		setup   func(*KubeModelSet)
		pod     *Pod
		wantErr string
		want    *KubeModelSet
	}{
		{
			name:    "validation failure",
			pod:     &Pod{UID: "", Name: "my-pod", NamespaceUID: "ns-uid", Start: start, End: end},
			wantErr: "RegisterPod: invalid pod: UID is missing for Pod with name 'my-pod'",
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				kms.Metadata.Diagnostics = []Diagnostic{
					{Level: DiagnosticLevelError, Message: "RegisterPod: invalid pod: UID is missing for Pod with name 'my-pod'"},
				}
				return kms
			}(),
		},
		{
			name: "warns when cluster is nil",
			pod:  newPod("pod-uid", "my-pod"),
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				kms.Pods["pod-uid"] = newPod("pod-uid", "my-pod")
				kms.Metadata.ObjectCount = 1
				kms.Metadata.Diagnostics = []Diagnostic{
					{Level: DiagnosticLevelWarning, Message: "RegisterPod: Cluster is nil"},
				}
				return kms
			}(),
		},
		{
			name:  "registers pod with cluster",
			setup: withCluster,
			pod:   newPod("pod-uid", "my-pod"),
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				withCluster(kms)
				kms.Pods["pod-uid"] = newPod("pod-uid", "my-pod")
				kms.Metadata.ObjectCount = 1
				return kms
			}(),
		},
		{
			name: "duplicate registration is a no-op",
			setup: func(kms *KubeModelSet) {
				withCluster(kms)
				kms.RegisterPod(newPod("pod-uid", "original"))
			},
			pod: newPod("pod-uid", "duplicate"),
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				withCluster(kms)
				kms.Pods["pod-uid"] = newPod("pod-uid", "original")
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

			err := kms.RegisterPod(tt.pod)

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}

			KubeModelSetEquals(t, tt.want, kms)
		})
	}
}
