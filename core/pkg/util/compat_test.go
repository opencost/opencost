package util

import (
	"testing"
)

func TestIsPreemptible(t *testing.T) {
	tests := map[string]struct {
		labels map[string]string
		want   bool
	}{
		"gke preemptible": {
			labels: map[string]string{"cloud.google.com/gke-preemptible": "true"},
			want:   true,
		},
		"gke spot": {
			labels: map[string]string{"cloud.google.com/gke-spot": "true"},
			want:   true,
		},
		"karpenter spot": {
			labels: map[string]string{"karpenter.sh/capacity-type": "spot"},
			want:   true,
		},
		"karpenter on-demand": {
			labels: map[string]string{"karpenter.sh/capacity-type": "on-demand"},
			want:   false,
		},
		"azure spot": {
			labels: map[string]string{"kubernetes.azure.com/scalesetpriority": "spot"},
			want:   true,
		},
		"azure regular": {
			labels: map[string]string{"kubernetes.azure.com/scalesetpriority": "regular"},
			want:   false,
		},
		"oci preemptible": {
			labels: map[string]string{"oci.oraclecloud.com/oke-is-preemptible": "true"},
			want:   true,
		},
		"on-demand node": {
			labels: map[string]string{"kubernetes.io/arch": "amd64"},
			want:   false,
		},
		"empty labels": {
			labels: map[string]string{},
			want:   false,
		},
		"nil labels": {
			labels: nil,
			want:   false,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := IsPreemptible(tt.labels); got != tt.want {
				t.Errorf("IsPreemptible() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPrivateIPCheck(t *testing.T) {
	tests := map[string]struct {
		ip   string
		want bool
	}{
		"RFC1918 10.x":        {ip: "10.0.0.1", want: true},
		"RFC1918 172.16.x":    {ip: "172.16.0.1", want: true},
		"RFC1918 172.31.x":    {ip: "172.31.255.255", want: true},
		"RFC1918 192.168.x":   {ip: "192.168.1.1", want: true},
		"public IPv4":         {ip: "8.8.8.8", want: false},
		"loopback":            {ip: "127.0.0.1", want: false},
		"private IPv6 fc00::": {ip: "fc00::1", want: true},
		"private IPv6 fd00::": {ip: "fd00::1", want: true},
		"public IPv6":         {ip: "2001:4860:4860::8888", want: false},
		"invalid IP":          {ip: "not-an-ip", want: false},
		"empty string":        {ip: "", want: false},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := PrivateIPCheck(tt.ip); got != tt.want {
				t.Errorf("PrivateIPCheck(%q) = %v, want %v", tt.ip, got, tt.want)
			}
		})
	}
}

func TestGetArchType(t *testing.T) {
	type args struct {
		labels map[string]string
	}
	tests := map[string]struct {
		args  args
		want  string
		found bool
	}{
		"amd64 beta": {
			args: args{
				labels: map[string]string{
					"beta.kubernetes.io/arch": "amd64",
				},
			},
			want:  "amd64",
			found: true,
		},
		"arm64 beta": {
			args: args{
				labels: map[string]string{
					"beta.kubernetes.io/arch": "arm64",
				},
			},
			want:  "arm64",
			found: true,
		},
		"amd64": {
			args: args{
				labels: map[string]string{
					"kubernetes.io/arch": "amd64",
				},
			},
			want:  "amd64",
			found: true,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, found := GetArchType(tt.args.labels)
			if found != tt.found {
				t.Errorf("GetArchType() error = %v, wantErr %v", found, tt.found)
				return
			}
			if got != tt.want {
				t.Errorf("GetArchType() got = %v, want %v", got, tt.want)
			}
		})
	}
}
