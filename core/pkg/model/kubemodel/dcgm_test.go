package kubemodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestValidateDCGMDevice(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Hour)
	end := start.Add(time.Hour)
	window := Window{Start: start, End: end}

	tests := []struct {
		name    string
		device  *DCGMDevice
		wantErr string
	}{
		{
			name:    "empty UUID",
			device:  &DCGMDevice{Device: "GPU-0", Start: start, End: end},
			wantErr: "UUID is missing for DCGMDevice with device 'GPU-0'",
		},
		{
			name:    "outside window",
			device:  &DCGMDevice{UUID: "gpu-uuid", Device: "GPU-0", Start: start.Add(-time.Hour), End: end},
			wantErr: checkWindow(window, start.Add(-time.Hour), end).Error(),
		},
		{
			name:   "valid",
			device: &DCGMDevice{UUID: "gpu-uuid", Device: "GPU-0", Start: start, End: end},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.device.ValidateDCGMDevice(window)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRegisterDCGMDevice(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Hour)
	end := start.Add(time.Hour)

	newDevice := func(uuid, device string) *DCGMDevice {
		return &DCGMDevice{UUID: uuid, Device: device, Start: start, End: end}
	}
	withCluster := func(kms *KubeModelSet) {
		kms.RegisterCluster(&Cluster{UID: "cluster-uid", Start: start, End: end})
	}

	tests := []struct {
		name    string
		setup   func(*KubeModelSet)
		device  *DCGMDevice
		wantErr string
		want    *KubeModelSet
	}{
		{
			name:    "validation failure",
			device:  &DCGMDevice{UUID: "", Device: "GPU-0", Start: start, End: end},
			wantErr: "RegisterDCGMDevice: invalid dcgm device: UUID is missing for DCGMDevice with device 'GPU-0'",
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				kms.Metadata.Diagnostics = []Diagnostic{
					{Level: DiagnosticLevelError, Message: "RegisterDCGMDevice: invalid dcgm device: UUID is missing for DCGMDevice with device 'GPU-0'"},
				}
				return kms
			}(),
		},
		{
			name:   "warns when cluster is nil",
			device: newDevice("gpu-uuid", "GPU-0"),
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				kms.DCGMDevices["gpu-uuid"] = newDevice("gpu-uuid", "GPU-0")
				kms.Metadata.ObjectCount = 1
				kms.Metadata.Diagnostics = []Diagnostic{
					{Level: DiagnosticLevelWarning, Message: "RegisterDCGMDevice: Cluster is nil"},
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
				kms.DCGMDevices["gpu-uuid"] = newDevice("gpu-uuid", "GPU-0")
				kms.Metadata.ObjectCount = 1
				return kms
			}(),
		},
		{
			name: "duplicate registration is a no-op",
			setup: func(kms *KubeModelSet) {
				withCluster(kms)
				kms.RegisterDCGMDevice(newDevice("gpu-uuid", "GPU-0"))
			},
			device: newDevice("gpu-uuid", "GPU-1"),
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				withCluster(kms)
				kms.DCGMDevices["gpu-uuid"] = newDevice("gpu-uuid", "GPU-0")
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

			err := kms.RegisterDCGMDevice(tt.device)

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}

			KubeModelSetEquals(t, tt.want, kms)
		})
	}
}
