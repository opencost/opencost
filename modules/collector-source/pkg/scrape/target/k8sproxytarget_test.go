package target

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type mockPodProxyGetter struct {
	response string
	err      error
}

func (m *mockPodProxyGetter) Get(ctx context.Context, namespace, podName string, port int, path string) (io.Reader, error) {
	if m.err != nil {
		return nil, m.err
	}
	return strings.NewReader(m.response), nil
}

func createMockServer(response string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(response))
	}))
}

func readResponse(t *testing.T, reader io.Reader) string {
	t.Helper()
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}
	return string(data)
}

func TestK8sProxyTarget_Load(t *testing.T) {
	tests := []struct {
		name           string
		setupServer    func() (url string, cleanup func())
		proxyGetter    PodProxyGetter
		expectedResult string
		expectError    bool
		errorContains  string
	}{
		{
			name: "direct HTTP success",
			setupServer: func() (string, func()) {
				server := createMockServer("direct response")
				return server.URL, server.Close
			},
			proxyGetter:    &mockPodProxyGetter{response: "proxy response"},
			expectedResult: "direct response",
		},
		{
			name: "proxy fallback on connection failure",
			setupServer: func() (string, func()) {
				server := createMockServer("unused")
				url := server.URL
				server.Close()
				return url, func() {}
			},
			proxyGetter:    &mockPodProxyGetter{response: "proxy response"},
			expectedResult: "proxy response",
		},
		{
			name: "both direct and proxy fail",
			setupServer: func() (string, func()) {
				server := createMockServer("unused")
				url := server.URL
				server.Close()
				return url, func() {}
			},
			proxyGetter:   &mockPodProxyGetter{err: errors.New("proxy error")},
			expectError:   true,
			errorContains: "both direct HTTP and K8s",
		},
		{
			name: "nil proxy getter",
			setupServer: func() (string, func()) {
				server := createMockServer("unused")
				url := server.URL
				server.Close()
				return url, func() {}
			},
			proxyGetter:   nil,
			expectError:   true,
			errorContains: "no proxy getter available",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url, cleanup := tt.setupServer()
			defer cleanup()

			target := NewK8sProxyTarget(url, tt.proxyGetter, "test-namespace", "test-pod", 3001, "metrics")
			reader, err := target.Load()

			if tt.expectError {
				if err == nil {
					t.Fatal("Expected error, got nil")
				}
				if !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("Expected error containing '%s', got: %s", tt.errorContains, err.Error())
				}
				return
			}

			if err != nil {
				t.Fatalf("Expected no error, got: %v", err)
			}

			result := readResponse(t, reader)
			if result != tt.expectedResult {
				t.Errorf("Expected '%s', got: %s", tt.expectedResult, result)
			}
		})
	}
}

func TestNewK8sProxyTarget(t *testing.T) {
	proxyGetter := &mockPodProxyGetter{response: "test"}
	target := NewK8sProxyTarget("http://10.0.0.1:3001/metrics", proxyGetter, "test-ns", "test-pod", 3001, "metrics")

	if target == nil {
		t.Fatal("Expected non-nil target")
	}
	if target.namespace != "test-ns" {
		t.Errorf("Expected namespace 'test-ns', got '%s'", target.namespace)
	}
	if target.podName != "test-pod" {
		t.Errorf("Expected podName 'test-pod', got '%s'", target.podName)
	}
	if target.port != 3001 {
		t.Errorf("Expected port 3001, got %d", target.port)
	}
	if target.path != "metrics" {
		t.Errorf("Expected path 'metrics', got '%s'", target.path)
	}
	if target.proxyGetter == nil {
		t.Error("Expected non-nil proxyGetter")
	}
	if target.directTarget == nil {
		t.Error("Expected non-nil directTarget")
	}
}

func TestK8sProxyTarget_ImplementsScrapeTarget(t *testing.T) {
	proxyGetter := &mockPodProxyGetter{response: "test"}
	var _ ScrapeTarget = NewK8sProxyTarget("http://10.0.0.1:3001/metrics", proxyGetter, "test-ns", "test-pod", 3001, "metrics")
}

func TestBytesReader(t *testing.T) {
	data := []byte("test data")
	reader := &bytesReader{data: data}

	buf := make([]byte, len(data))
	n, err := reader.Read(buf)
	if err != nil || n != len(data) || string(buf) != string(data) {
		t.Errorf("Expected to read '%s', got '%s' (n=%d, err=%v)", string(data), string(buf), n, err)
	}

	// EOF on next read
	n, err = reader.Read(buf)
	if err != io.EOF || n != 0 {
		t.Errorf("Expected EOF with 0 bytes, got n=%d, err=%v", n, err)
	}
}
