package clustercache

import (
	"testing"

	v1 "k8s.io/api/core/v1"
)

func TestGetLoadBalancerIngressAddress(t *testing.T) {
	tests := []struct {
		name     string
		service  *Service
		expected []string
	}{
		{
			name:     "no ingresses",
			service:  &Service{},
			expected: nil,
		},
		{
			name: "single IP ingress",
			service: &Service{
				Status: v1.ServiceStatus{
					LoadBalancer: v1.LoadBalancerStatus{
						Ingress: []v1.LoadBalancerIngress{
							{IP: "1.2.3.4"},
						},
					},
				},
			},
			expected: []string{"1.2.3.4"},
		},
		{
			name: "single hostname ingress",
			service: &Service{
				Status: v1.ServiceStatus{
					LoadBalancer: v1.LoadBalancerStatus{
						Ingress: []v1.LoadBalancerIngress{
							{Hostname: "lb.example.com"},
						},
					},
				},
			},
			expected: []string{"lb.example.com"},
		},
		{
			name: "IP takes priority over hostname",
			service: &Service{
				Status: v1.ServiceStatus{
					LoadBalancer: v1.LoadBalancerStatus{
						Ingress: []v1.LoadBalancerIngress{
							{IP: "1.2.3.4", Hostname: "lb.example.com"},
						},
					},
				},
			},
			expected: []string{"1.2.3.4"},
		},
		{
			name: "multiple ingresses",
			service: &Service{
				Status: v1.ServiceStatus{
					LoadBalancer: v1.LoadBalancerStatus{
						Ingress: []v1.LoadBalancerIngress{
							{IP: "1.2.3.4"},
							{Hostname: "lb2.example.com"},
							{IP: "5.6.7.8"},
						},
					},
				},
			},
			expected: []string{"1.2.3.4", "lb2.example.com", "5.6.7.8"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetLoadBalancerIngressAddress(tt.service)
			if len(got) != len(tt.expected) {
				t.Fatalf("got %v, want %v", got, tt.expected)
			}
			for i := range tt.expected {
				if got[i] != tt.expected[i] {
					t.Errorf("index %d: got %q, want %q", i, got[i], tt.expected[i])
				}
			}
		})
	}
}

func Test_getPVProviderID(t *testing.T) {
	tests := []struct {
		name string
		pv   *PersistentVolume
		want string
	}{
		{
			name: "gce persistent disk uses pd name",
			pv: &PersistentVolume{
				Name: "pv-gce",
				Spec: v1.PersistentVolumeSpec{
					PersistentVolumeSource: v1.PersistentVolumeSource{
						GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{PDName: "gke-pd-1"},
					},
				},
			},
			want: "gke-pd-1",
		},
		{
			name: "azure disk uses disk name",
			pv: &PersistentVolume{
				Name: "pv-azure",
				Spec: v1.PersistentVolumeSpec{
					PersistentVolumeSource: v1.PersistentVolumeSource{
						AzureDisk: &v1.AzureDiskVolumeSource{DiskName: "azure-disk-1"},
					},
				},
			},
			want: "azure-disk-1",
		},
		{
			name: "aws ebs with aws:// prefixed volume id is parsed",
			pv: &PersistentVolume{
				Name: "pv-aws",
				Spec: v1.PersistentVolumeSpec{
					PersistentVolumeSource: v1.PersistentVolumeSource{
						AWSElasticBlockStore: &v1.AWSElasticBlockStoreVolumeSource{
							VolumeID: "aws://us-east-2a/vol-0fc54c5e83b8d2b76",
						},
					},
				},
			},
			want: "vol-0fc54c5e83b8d2b76",
		},
		{
			name: "aws ebs with bare volume id is left unchanged",
			pv: &PersistentVolume{
				Name: "pv-aws",
				Spec: v1.PersistentVolumeSpec{
					PersistentVolumeSource: v1.PersistentVolumeSource{
						AWSElasticBlockStore: &v1.AWSElasticBlockStoreVolumeSource{
							VolumeID: "vol-abc123",
						},
					},
				},
			},
			want: "vol-abc123",
		},
		{
			name: "aws ebs with empty volume id yields empty string",
			pv: &PersistentVolume{
				Name: "pv-aws",
				Spec: v1.PersistentVolumeSpec{
					PersistentVolumeSource: v1.PersistentVolumeSource{
						AWSElasticBlockStore: &v1.AWSElasticBlockStoreVolumeSource{VolumeID: ""},
					},
				},
			},
			want: "",
		},
		{
			name: "csi uses volume handle",
			pv: &PersistentVolume{
				Name: "pv-csi",
				Spec: v1.PersistentVolumeSpec{
					PersistentVolumeSource: v1.PersistentVolumeSource{
						CSI: &v1.CSIPersistentVolumeSource{VolumeHandle: "vol-csi-1"},
					},
				},
			},
			want: "vol-csi-1",
		},
		{
			// Documents current behavior: a CSI source with an empty handle
			// returns "" rather than falling back to pv.Name.
			name: "csi with empty volume handle returns empty string",
			pv: &PersistentVolume{
				Name: "pv-csi",
				Spec: v1.PersistentVolumeSpec{
					PersistentVolumeSource: v1.PersistentVolumeSource{
						CSI: &v1.CSIPersistentVolumeSource{VolumeHandle: ""},
					},
				},
			},
			want: "",
		},
		{
			name: "no recognized source falls back to pv name",
			pv: &PersistentVolume{
				Name: "pv-nfs",
				Spec: v1.PersistentVolumeSpec{},
			},
			want: "pv-nfs",
		},
		{
			// GCE branch is checked before CSI, so GCE wins when both are set.
			name: "gce takes precedence over csi",
			pv: &PersistentVolume{
				Name: "pv-both",
				Spec: v1.PersistentVolumeSpec{
					PersistentVolumeSource: v1.PersistentVolumeSource{
						GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{PDName: "gce-wins"},
						CSI:               &v1.CSIPersistentVolumeSource{VolumeHandle: "csi-loses"},
					},
				},
			},
			want: "gce-wins",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetPVProviderID(tt.pv); got != tt.want {
				t.Errorf("getPVProviderID() = %q, want %q", got, tt.want)
			}
		})
	}
}

func Test_persistentVolumeAWSRegex(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string // expected capture group 1, or "" for no match
	}{
		{
			name:  "standard aws:// volume id",
			input: "aws://us-east-2a/vol-0fc54c5e83b8d2b76",
			want:  "vol-0fc54c5e83b8d2b76",
		},
		{
			name:  "trailing path segment stops at slash",
			input: "aws://us-east-2a/vol-123/extra",
			want:  "vol-123",
		},
		{
			name:  "bare volume id does not match",
			input: "vol-abc123",
			want:  "",
		},
		{
			name:  "too few segments does not match",
			input: "aws://vol-123",
			want:  "",
		},
		{
			name:  "empty string does not match",
			input: "",
			want:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			match := persistentVolumeAWSRegex.FindStringSubmatch(tt.input)
			got := ""
			if len(match) >= 2 {
				got = match[1]
			}
			if got != tt.want {
				t.Errorf("persistentVolumeAWSRegex on %q = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}
