package kubemodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestValidateDevice(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Hour)
	end := start.Add(time.Hour)
	window := Window{Start: start, End: end}

	tests := []struct {
		name    string
		device  *Device
		wantErr string
	}{
		{
			name:    "empty UUID",
			device:  &Device{Device: "GPU-0", Start: start, End: end},
			wantErr: "UUID is missing for Device with device 'GPU-0'",
		},
		{
			name:    "outside window",
			device:  &Device{UUID: "gpu-uuid", Device: "GPU-0", Start: start.Add(-time.Hour), End: end},
			wantErr: checkWindow(window, start.Add(-time.Hour), end).Error(),
		},
		{
			name:   "valid",
			device: &Device{UUID: "gpu-uuid", Device: "GPU-0", Start: start, End: end},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.device.ValidateDevice(window)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRegisterDevice(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Hour)
	end := start.Add(time.Hour)

	newDevice := func(uuid, device string) *Device {
		return &Device{UUID: uuid, Device: device, Start: start, End: end}
	}
	withCluster := func(kms *KubeModelSet) {
		kms.RegisterCluster(&Cluster{UID: "cluster-uid", Start: start, End: end})
	}

	tests := []struct {
		name    string
		setup   func(*KubeModelSet)
		device  *Device
		wantErr string
		want    *KubeModelSet
	}{
		{
			name:    "validation failure",
			device:  &Device{UUID: "", Device: "GPU-0", Start: start, End: end},
			wantErr: "RegisterDevice: invalid device: UUID is missing for Device with device 'GPU-0'",
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				kms.Metadata.Diagnostics = []Diagnostic{
					{Level: DiagnosticLevelError, Message: "RegisterDevice: invalid device: UUID is missing for Device with device 'GPU-0'"},
				}
				return kms
			}(),
		},
		{
			name:   "warns when cluster is nil",
			device: newDevice("gpu-uuid", "GPU-0"),
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				kms.Devices["gpu-uuid"] = newDevice("gpu-uuid", "GPU-0")
				kms.Metadata.ObjectCount = 1
				kms.Metadata.Diagnostics = []Diagnostic{
					{Level: DiagnosticLevelWarning, Message: "RegisterDevice: Cluster is nil"},
				}
				return kms
			}(),
		},
		{
			name:   "registers device with cluster",
			setup:  withCluster,
			device: newDevice("gpu-uuid", "GPU-0"),
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				withCluster(kms)
				kms.Devices["gpu-uuid"] = newDevice("gpu-uuid", "GPU-0")
				kms.Metadata.ObjectCount = 1
				return kms
			}(),
		},
		{
			name: "duplicate registration is a no-op",
			setup: func(kms *KubeModelSet) {
				withCluster(kms)
				kms.RegisterDevice(newDevice("gpu-uuid", "GPU-0"))
			},
			device: newDevice("gpu-uuid", "GPU-1"),
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				withCluster(kms)
				kms.Devices["gpu-uuid"] = newDevice("gpu-uuid", "GPU-0")
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

			err := kms.RegisterDevice(tt.device)

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}

			KubeModelSetEquals(t, tt.want, kms)
		})
	}
}
