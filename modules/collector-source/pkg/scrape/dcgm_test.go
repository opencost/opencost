package scrape

import (
	"testing"
)

func Test_isDCGM(t *testing.T) {
	tests := map[string]struct {
		labels map[string]string
		want   bool
	}{
		"nil": {
			labels: nil,
			want:   false,
		},
		"empty": {
			labels: map[string]string{},
			want:   false,
		},
		"app": {
			labels: map[string]string{
				"app": "dcgm-exporter",
			},
			want: true,
		},
		"app.kubernetes.io/name": {
			labels: map[string]string{
				"app.kubernetes.io/name": "dcgm-exporter",
			},
			want: true,
		},
		"app.kubernetes.io/component": {
			labels: map[string]string{
				"app.kubernetes.io/name": "dcgm-exporter",
			},
			want: true,
		},
		"invalid key": {
			labels: map[string]string{
				"invalid-key": "dcgm-exporter",
			},
			want: false,
		},
		"invalid value": {
			labels: map[string]string{
				"app.kubernetes.io/name": "dcgmExporter",
			},
			want: false,
		},
		"case insensitive": {
			labels: map[string]string{
				"app.kubernetes.io/name": "jhlkjhlkDcGm-eXpoRterlkjhlkuh",
			},
			want: true,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := isDCGM(tt.labels); got != tt.want {
				t.Errorf("isDCGM() = %v, want %v", got, tt.want)
			}
		})
	}
}
