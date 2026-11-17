package nodestats

import (
	"net/url"
	"testing"

	"github.com/opencost/opencost/core/pkg/clustercache"
	v1 "k8s.io/api/core/v1"
)

func TestDirectNodeFormatter_FormatEndpoint(t *testing.T) {
	tests := []struct {
		name     string
		ip       string
		port     int32
		endpoint string
		want     string
	}{
		{
			name:     "ipv4",
			ip:       "192.0.2.10",
			port:     10250,
			endpoint: "stats/summary",
			want:     "https://192.0.2.10:10250/stats/summary",
		},
		{
			name:     "ipv6",
			ip:       "2001:db8:1:2::a",
			port:     10250,
			endpoint: "stats/summary",
			want:     "https://[2001:db8:1:2::a]:10250/stats/summary",
		},
		{
			name:     "ipv6 cadvisor",
			ip:       "2001:db8::1",
			port:     10250,
			endpoint: "metrics/cadvisor",
			want:     "https://[2001:db8::1]:10250/metrics/cadvisor",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := &clustercache.Node{
				Name: "node-" + tt.name,
				Status: v1.NodeStatus{
					Addresses: []v1.NodeAddress{
						{Type: v1.NodeHostName, Address: "node-" + tt.name},
						{Type: v1.NodeInternalIP, Address: tt.ip},
					},
					DaemonEndpoints: v1.NodeDaemonEndpoints{
						KubeletEndpoint: v1.DaemonEndpoint{Port: tt.port},
					},
				},
			}

			formatter, err := NewDirectNodeFormatterFrom(node)
			if err != nil {
				t.Fatalf("NewDirectNodeFormatterFrom() error = %v", err)
			}

			got := formatter.FormatEndpoint(tt.endpoint)
			if got != tt.want {
				t.Errorf("FormatEndpoint() = %q, want %q", got, tt.want)
			}

			// The URL must be parseable: http.NewRequest fails with "invalid port" on an unbracketed IPv6 host
			u, err := url.Parse(got)
			if err != nil {
				t.Fatalf("url.Parse(%q) error = %v", got, err)
			}
			if u.Hostname() != tt.ip {
				t.Errorf("parsed hostname = %q, want %q", u.Hostname(), tt.ip)
			}
		})
	}
}
