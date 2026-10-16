package util

import (
	"testing"

	v1 "k8s.io/api/core/v1"
)

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

func TestGetZone(t *testing.T) {
	type args struct {
		labels map[string]string
	}
	tests := map[string]struct {
		args  args
		want  string
		found bool
	}{
		"stable label": {
			args:  args{labels: map[string]string{v1.LabelTopologyZone: "us-east-1a"}},
			want:  "us-east-1a",
			found: true,
		},
		"deprecated label": {
			args:  args{labels: map[string]string{v1.LabelZoneFailureDomain: "us-east-1b"}},
			want:  "us-east-1b",
			found: true,
		},
		"both labels prefers stable": {
			args: args{labels: map[string]string{
				v1.LabelTopologyZone:      "us-east-1a",
				v1.LabelZoneFailureDomain: "us-east-1b",
			}},
			want:  "us-east-1a",
			found: true,
		},
		"no matching labels": {
			args:  args{labels: map[string]string{"unrelated": "value"}},
			want:  "",
			found: false,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, found := GetZone(tt.args.labels)
			if found != tt.found {
				t.Errorf("GetZone() found = %v, want %v", found, tt.found)
				return
			}
			if got != tt.want {
				t.Errorf("GetZone() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetRegion(t *testing.T) {
	type args struct {
		labels map[string]string
	}
	tests := map[string]struct {
		args  args
		want  string
		found bool
	}{
		"stable label": {
			args:  args{labels: map[string]string{v1.LabelTopologyRegion: "us-east-1"}},
			want:  "us-east-1",
			found: true,
		},
		"deprecated label": {
			args:  args{labels: map[string]string{v1.LabelZoneRegion: "us-west-2"}},
			want:  "us-west-2",
			found: true,
		},
		"both labels prefers stable": {
			args: args{labels: map[string]string{
				v1.LabelTopologyRegion: "us-east-1",
				v1.LabelZoneRegion:     "us-west-2",
			}},
			want:  "us-east-1",
			found: true,
		},
		"no matching labels": {
			args:  args{labels: map[string]string{"unrelated": "value"}},
			want:  "",
			found: false,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, found := GetRegion(tt.args.labels)
			if found != tt.found {
				t.Errorf("GetRegion() found = %v, want %v", found, tt.found)
				return
			}
			if got != tt.want {
				t.Errorf("GetRegion() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetInstanceType(t *testing.T) {
	type args struct {
		labels map[string]string
	}
	tests := map[string]struct {
		args  args
		want  string
		found bool
	}{
		"stable label": {
			args:  args{labels: map[string]string{v1.LabelInstanceTypeStable: "m5.xlarge"}},
			want:  "m5.xlarge",
			found: true,
		},
		"deprecated label": {
			args:  args{labels: map[string]string{v1.LabelInstanceType: "m4.large"}},
			want:  "m4.large",
			found: true,
		},
		"both labels prefers stable": {
			args: args{labels: map[string]string{
				v1.LabelInstanceTypeStable: "m5.xlarge",
				v1.LabelInstanceType:       "m4.large",
			}},
			want:  "m5.xlarge",
			found: true,
		},
		"no matching labels": {
			args:  args{labels: map[string]string{"unrelated": "value"}},
			want:  "",
			found: false,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, found := GetInstanceType(tt.args.labels)
			if found != tt.found {
				t.Errorf("GetInstanceType() found = %v, want %v", found, tt.found)
				return
			}
			if got != tt.want {
				t.Errorf("GetInstanceType() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetOperatingSystem(t *testing.T) {
	type args struct {
		labels map[string]string
	}
	tests := map[string]struct {
		args  args
		want  string
		found bool
	}{
		"stable label": {
			args:  args{labels: map[string]string{v1.LabelOSStable: "linux"}},
			want:  "linux",
			found: true,
		},
		"beta label": {
			args:  args{labels: map[string]string{"beta.kubernetes.io/os": "windows"}},
			want:  "windows",
			found: true,
		},
		"both labels prefers stable": {
			args: args{labels: map[string]string{
				v1.LabelOSStable:       "linux",
				"beta.kubernetes.io/os": "windows",
			}},
			want:  "linux",
			found: true,
		},
		"no matching labels": {
			args:  args{labels: map[string]string{"unrelated": "value"}},
			want:  "",
			found: false,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, found := GetOperatingSystem(tt.args.labels)
			if found != tt.found {
				t.Errorf("GetOperatingSystem() found = %v, want %v", found, tt.found)
				return
			}
			if got != tt.want {
				t.Errorf("GetOperatingSystem() got = %v, want %v", got, tt.want)
			}
		})
	}
}
