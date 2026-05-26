package storage

import (
	"crypto/tls"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
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

func TestClusterStorage_ReadToLocalFile(t *testing.T) {
	expected := []byte("cluster-storage-contents")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/clusterStorage/read" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		resp := Response[[]byte]{
			Code: 0,
			Data: expected,
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parsing test server URL: %s", err)
	}

	port, err := strconv.Atoi(u.Port())
	if err != nil {
		t.Fatalf("parsing test server port: %s", err)
	}

	cs := &ClusterStorage{
		client: &http.Client{},
		host:   u.Hostname(),
		port:   port,
	}

	destPath := filepath.Join(t.TempDir(), "out.bin")
	if err := cs.ReadToLocalFile("some/path", destPath); err != nil {
		t.Fatalf("ReadToLocalFile failed: %s", err)
	}

	data, err := os.ReadFile(destPath)
	if err != nil {
		t.Fatalf("reading destination file: %s", err)
	}

	if string(data) != string(expected) {
		t.Fatalf("destination file contents mismatch: got %q want %q", string(data), string(expected))
	}
}

func TestClusterStorage_ReadStream(t *testing.T) {
	expected := []byte("cluster-storage-stream-contents")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/clusterStorage/read" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		resp := Response[[]byte]{
			Code: 0,
			Data: expected,
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parsing test server URL: %s", err)
	}

	port, err := strconv.Atoi(u.Port())
	if err != nil {
		t.Fatalf("parsing test server port: %s", err)
	}

	cs := &ClusterStorage{
		client: &http.Client{},
		host:   u.Hostname(),
		port:   port,
	}

	r, err := cs.ReadStream("some/path")
	if err != nil {
		t.Fatalf("ReadStream failed: %s", err)
	}
	defer r.Close()

	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("reading stream failed: %s", err)
	}

	if string(data) != string(expected) {
		t.Fatalf("stream contents mismatch: got %q want %q", string(data), string(expected))
	}
}
