package kubemodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestValidatePVC(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Hour)
	end := start.Add(time.Hour)
	window := Window{Start: start, End: end}

	tests := []struct {
		name    string
		pvc     *PersistentVolumeClaim
		wantErr string
	}{
		{
			name:    "empty UID",
			pvc:     &PersistentVolumeClaim{Name: "my-pvc", NamespaceUID: "ns-uid", Start: start, End: end},
			wantErr: "UID is missing for PVC with name 'my-pvc'",
		},
		{
			name:    "empty Name",
			pvc:     &PersistentVolumeClaim{UID: "pvc-uid", NamespaceUID: "ns-uid", Start: start, End: end},
			wantErr: "Name is missing for PVC 'pvc-uid'",
		},
		{
			name:    "empty NamespaceUID",
			pvc:     &PersistentVolumeClaim{UID: "pvc-uid", Name: "my-pvc", Start: start, End: end},
			wantErr: "NamespaceUID is missing for PVC 'pvc-uid'",
		},
		{
			name:    "outside window",
			pvc:     &PersistentVolumeClaim{UID: "pvc-uid", Name: "my-pvc", NamespaceUID: "ns-uid", Start: start.Add(-time.Hour), End: end},
			wantErr: checkWindow(window, start.Add(-time.Hour), end).Error(),
		},
		{
			name: "valid",
			pvc:  &PersistentVolumeClaim{UID: "pvc-uid", Name: "my-pvc", NamespaceUID: "ns-uid", Start: start, End: end},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.pvc.ValidatePVC(window)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRegisterPVC(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Hour)
	end := start.Add(time.Hour)

	newPVC := func(uid, name string) *PersistentVolumeClaim {
		return &PersistentVolumeClaim{UID: uid, Name: name, NamespaceUID: "ns-uid", Start: start, End: end}
	}

	tests := []struct {
		name    string
		setup   func(*KubeModelSet)
		pvc     *PersistentVolumeClaim
		wantErr string
		want    *KubeModelSet
	}{
		{
			name:    "validation failure",
			pvc:     &PersistentVolumeClaim{UID: "", Name: "my-pvc", NamespaceUID: "ns-uid", Start: start, End: end},
			wantErr: "RegisterPVC: invalid pvc: UID is missing for PVC with name 'my-pvc'",
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				kms.Metadata.Diagnostics = []Diagnostic{
					{Level: DiagnosticLevelError, Message: "RegisterPVC: invalid pvc: UID is missing for PVC with name 'my-pvc'"},
				}
				return kms
			}(),
		},
		{
			name: "registers pvc",
			pvc:  newPVC("pvc-uid", "my-pvc"),
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				kms.PersistentVolumeClaims["pvc-uid"] = newPVC("pvc-uid", "my-pvc")
				kms.Metadata.ObjectCount = 1
				return kms
			}(),
		},
		{
			name: "duplicate registration is a no-op",
			setup: func(kms *KubeModelSet) {
				kms.RegisterPVC(newPVC("pvc-uid", "original"))
			},
			pvc: newPVC("pvc-uid", "duplicate"),
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				kms.PersistentVolumeClaims["pvc-uid"] = newPVC("pvc-uid", "original")
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

			err := kms.RegisterPVC(tt.pvc)

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}

			KubeModelSetEquals(t, tt.want, kms)
		})
	}
}
