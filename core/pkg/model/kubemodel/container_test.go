package kubemodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestValidateContainer(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Hour)
	end := start.Add(time.Hour)
	window := Window{Start: start, End: end}

	tests := []struct {
		name      string
		container *Container
		wantErr   string
	}{
		{
			name:      "empty PodUID",
			container: &Container{Name: "my-container", Start: start, End: end},
			wantErr:   "PodUID is missing for Container with name 'my-container'",
		},
		{
			name:      "empty Name",
			container: &Container{PodUID: "pod-uid", Start: start, End: end},
			wantErr:   "Name is missing for Container on pod 'pod-uid'",
		},
		{
			name:      "outside window",
			container: &Container{PodUID: "pod-uid", Name: "my-container", Start: start.Add(-time.Hour), End: end},
			wantErr:   checkWindow(window, start.Add(-time.Hour), end).Error(),
		},
		{
			name:      "valid",
			container: &Container{PodUID: "pod-uid", Name: "my-container", Start: start, End: end},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.container.ValidateContainer(window)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRegisterContainer(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Hour)
	end := start.Add(time.Hour)

	newContainer := func(podUID, name string) *Container {
		return &Container{PodUID: podUID, Name: name, Start: start, End: end}
	}

	tests := []struct {
		name      string
		setup     func(*KubeModelSet)
		container *Container
		wantErr   string
		want      *KubeModelSet
	}{
		{
			name:      "validation failure",
			container: &Container{PodUID: "", Name: "my-container", Start: start, End: end},
			wantErr:   "RegisterContainer: invalid container: PodUID is missing for Container with name 'my-container'",
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				kms.Metadata.Diagnostics = []Diagnostic{
					{Level: DiagnosticLevelError, Message: "RegisterContainer: invalid container: PodUID is missing for Container with name 'my-container'"},
				}
				return kms
			}(),
		},
		{
			name:      "registers container",
			container: newContainer("pod-uid", "my-container"),
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				kms.Containers["pod-uid/my-container"] = newContainer("pod-uid", "my-container")
				kms.Metadata.ObjectCount = 1
				return kms
			}(),
		},
		{
			name: "duplicate registration is a no-op",
			setup: func(kms *KubeModelSet) {
				kms.RegisterContainer(newContainer("pod-uid", "original"))
			},
			container: newContainer("pod-uid", "original"),
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				kms.Containers["pod-uid/original"] = newContainer("pod-uid", "original")
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

			err := kms.RegisterContainer(tt.container)

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}

			KubeModelSetEquals(t, tt.want, kms)
		})
	}
}
