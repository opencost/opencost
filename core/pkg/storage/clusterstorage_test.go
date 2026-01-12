package storage

import (
	"crypto/tls"
	"net/http"
	"strings"
	"testing"
)

// TestClusterStorage_scheme tests the scheme() method returns correct values based on TLS configuration
func TestClusterStorage_scheme(t *testing.T) {
	tests := []struct {
		name      string
		transport http.RoundTripper
		want      string
	}{
		{
			name:      "nil transport returns http",
			transport: nil,
			want:      "http",
		},
		{
			name:      "transport without TLS config returns http",
			transport: &http.Transport{},
			want:      "http",
		},
		{
			name: "transport with TLS config returns https",
			transport: &http.Transport{
				TLSClientConfig: &tls.Config{},
			},
			want: "https",
		},
		{
			name: "transport with InsecureSkipVerify returns http",
			transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
			},
			want: "http",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cs := &ClusterStorage{
				client: &http.Client{
					Transport: tt.transport,
				},
			}
			got := cs.scheme()
			if got != tt.want {
				t.Errorf("ClusterStorage.scheme() = %v, want %v", got, tt.want)
			}

			// Also test that strings.ToUpper(scheme()) works as expected in log statements
			gotUpper := strings.ToUpper(cs.scheme())
			wantUpper := strings.ToUpper(tt.want)
			if gotUpper != wantUpper {
				t.Errorf("strings.ToUpper(ClusterStorage.scheme()) = %v, want %v", gotUpper, wantUpper)
			}
		})
	}
}
