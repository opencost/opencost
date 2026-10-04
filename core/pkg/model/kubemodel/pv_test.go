package kubemodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestValidatePersistentVolume(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Hour)
	end := start.Add(time.Hour)
	window := Window{Start: start, End: end}

	tests := []struct {
		name    string
		pv      *PersistentVolume
		wantErr string
	}{
		{
			name:    "empty UID",
			pv:      &PersistentVolume{Name: "my-pv", Start: start, End: end},
			wantErr: "UID is missing for PersistentVolume with name 'my-pv'",
		},
		{
			name:    "empty Name",
			pv:      &PersistentVolume{UID: "pv-uid", Start: start, End: end},
			wantErr: "Name is missing for PersistentVolume 'pv-uid'",
		},
		{
			name:    "outside window",
			pv:      &PersistentVolume{UID: "pv-uid", Name: "my-pv", Start: start.Add(-time.Hour), End: end},
			wantErr: checkWindow(window, start.Add(-time.Hour), end).Error(),
		},
		{
			name: "valid",
			pv:   &PersistentVolume{UID: "pv-uid", Name: "my-pv", Start: start, End: end},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.pv.ValidatePersistentVolume(window)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRegisterPersistentVolume(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Hour)
	end := start.Add(time.Hour)

	newPV := func(uid, name string) *PersistentVolume {
		return &PersistentVolume{UID: uid, Name: name, Start: start, End: end}
	}

	tests := []struct {
		name    string
		setup   func(*KubeModelSet)
		pv      *PersistentVolume
		wantErr string
		want    *KubeModelSet
	}{
		{
			name:    "validation failure",
			pv:      &PersistentVolume{UID: "", Name: "my-pv", Start: start, End: end},
			wantErr: "RegisterPersistentVolume: invalid persistent volume: UID is missing for PersistentVolume with name 'my-pv'",
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				kms.Metadata.Diagnostics = []Diagnostic{
					{Level: DiagnosticLevelError, Message: "RegisterPersistentVolume: invalid persistent volume: UID is missing for PersistentVolume with name 'my-pv'"},
				}
				return kms
			}(),
		},
		{
			name: "registers persistent volume",
			pv:   newPV("pv-uid", "my-pv"),
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				kms.PersistentVolumes["pv-uid"] = newPV("pv-uid", "my-pv")
				kms.Metadata.ObjectCount = 1
				return kms
			}(),
		},
		{
			name: "duplicate registration is a no-op",
			setup: func(kms *KubeModelSet) {
				kms.RegisterPersistentVolume(newPV("pv-uid", "original"))
			},
			pv: newPV("pv-uid", "duplicate"),
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				kms.PersistentVolumes["pv-uid"] = newPV("pv-uid", "original")
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

			err := kms.RegisterPersistentVolume(tt.pv)

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}

			KubeModelSetEquals(t, tt.want, kms)
		})
	}
}
