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

// mockPodProxyGetter implements PodProxyGetter for testing
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

// capturingPodProxyGetter captures parameters passed to Get method
type capturingPodProxyGetter struct {
	response          string
	capturedNamespace *string
	capturedPodName   *string
	capturedPort      *int
	capturedPath      *string
}

func (c *capturingPodProxyGetter) Get(ctx context.Context, namespace, podName string, port int, path string) (io.Reader, error) {
	*c.capturedNamespace = namespace
	*c.capturedPodName = podName
	*c.capturedPort = port
	*c.capturedPath = path
	return strings.NewReader(c.response), nil
}

// createMockServer creates a test HTTP server that returns the given response
func createMockServer(response string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(response))
	}))
}

func TestK8sProxyTarget_Load_DirectSuccess(t *testing.T) {
	// Create a mock HTTP server
	server := createMockServer("direct response")
	defer server.Close()

	// Create mock proxy getter (should not be called)
	proxyGetter := &mockPodProxyGetter{response: "proxy response"}

	// Create K8sProxyTarget
	target := NewK8sProxyTarget(
		server.URL,
		proxyGetter,
		"test-namespace",
		"test-pod",
		3001,
		"metrics",
	)

	// Load should succeed with direct HTTP
	reader, err := target.Load()
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Verify response
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	if string(data) != "direct response" {
		t.Errorf("Expected 'direct response', got: %s", string(data))
	}
}

func TestK8sProxyTarget_Load_ProxyFallback(t *testing.T) {
	// Use invalid URL to force direct HTTP to fail
	invalidURL := "http://invalid-host-that-does-not-exist:9999/metrics"

	// Create mock proxy getter that succeeds
	proxyGetter := &mockPodProxyGetter{response: "proxy response"}

	// Create K8sProxyTarget
	target := NewK8sProxyTarget(
		invalidURL,
		proxyGetter,
		"test-namespace",
		"test-pod",
		3001,
		"metrics",
	)

	// Load should succeed via proxy fallback
	reader, err := target.Load()
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Verify response
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	if string(data) != "proxy response" {
		t.Errorf("Expected 'proxy response', got: %s", string(data))
	}
}

func TestK8sProxyTarget_Load_BothFail(t *testing.T) {
	// Use invalid URL to force direct HTTP to fail
	invalidURL := "http://invalid-host-that-does-not-exist:9999/metrics"

	// Create mock proxy getter that also fails
	proxyGetter := &mockPodProxyGetter{err: errors.New("proxy error")}

	// Create K8sProxyTarget
	target := NewK8sProxyTarget(
		invalidURL,
		proxyGetter,
		"test-namespace",
		"test-pod",
		3001,
		"metrics",
	)

	// Load should fail with both errors
	_, err := target.Load()
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	// Error should mention both failures
	errMsg := err.Error()
	if !strings.Contains(errMsg, "both direct HTTP and K8s") {
		t.Errorf("Expected error message to mention both failures, got: %s", errMsg)
	}
}

func TestK8sProxyTarget_Load_ProxyParameters(t *testing.T) {
	// Use invalid URL to force proxy fallback
	invalidURL := "http://invalid-host:9999/metrics"

	// Track what parameters were passed to proxy
	var capturedNamespace, capturedPodName, capturedPath string
	var capturedPort int

	proxyGetter := &capturingPodProxyGetter{
		response:          "success",
		capturedNamespace: &capturedNamespace,
		capturedPodName:   &capturedPodName,
		capturedPort:      &capturedPort,
		capturedPath:      &capturedPath,
	}

	// Create K8sProxyTarget with specific parameters
	target := NewK8sProxyTarget(
		invalidURL,
		proxyGetter,
		"my-namespace",
		"my-pod",
		8080,
		"custom/path",
	)

	// Load to trigger proxy
	_, err := target.Load()
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Verify parameters were passed correctly
	if capturedNamespace != "my-namespace" {
		t.Errorf("Expected namespace 'my-namespace', got: %s", capturedNamespace)
	}
	if capturedPodName != "my-pod" {
		t.Errorf("Expected pod name 'my-pod', got: %s", capturedPodName)
	}
	if capturedPort != 8080 {
		t.Errorf("Expected port 8080, got: %d", capturedPort)
	}
	if capturedPath != "custom/path" {
		t.Errorf("Expected path 'custom/path', got: %s", capturedPath)
	}
}

func TestNewK8sProxyTarget(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		podName   string
		port      int
		path      string
	}{
		{
			name:      "basic metrics path",
			namespace: "opencost",
			podName:   "network-costs-abc123",
			port:      3001,
			path:      "metrics",
		},
		{
			name:      "path with leading slash",
			namespace: "monitoring",
			podName:   "exporter-xyz789",
			port:      9090,
			path:      "/metrics",
		},
		{
			name:      "custom path",
			namespace: "default",
			podName:   "custom-pod",
			port:      8080,
			path:      "api/v1/metrics",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proxyGetter := &mockPodProxyGetter{response: "test"}
			url := "http://10.0.0.1:3001/metrics"
			target := NewK8sProxyTarget(url, proxyGetter, tt.namespace, tt.podName, tt.port, tt.path)

			if target == nil {
				t.Fatal("Expected non-nil target")
			}

			if target.namespace != tt.namespace {
				t.Errorf("Expected namespace '%s', got '%s'", tt.namespace, target.namespace)
			}

			if target.podName != tt.podName {
				t.Errorf("Expected podName '%s', got '%s'", tt.podName, target.podName)
			}

			if target.port != tt.port {
				t.Errorf("Expected port %d, got %d", tt.port, target.port)
			}

			if target.path != tt.path {
				t.Errorf("Expected path '%s', got '%s'", tt.path, target.path)
			}

			if target.proxyGetter == nil {
				t.Error("Expected non-nil proxyGetter")
			}

			if target.directTarget == nil {
				t.Error("Expected non-nil directTarget")
			}
		})
	}
}

// TestK8sProxyTarget_ImplementsScrapeTarget verifies K8sProxyTarget implements ScrapeTarget
func TestK8sProxyTarget_ImplementsScrapeTarget(t *testing.T) {
	proxyGetter := &mockPodProxyGetter{response: "test"}
	var _ ScrapeTarget = NewK8sProxyTarget("http://10.0.0.1:3001/metrics", proxyGetter, "test-ns", "test-pod", 3001, "metrics")
}

// TestK8sProxyTarget_PathHandling tests various path formats
func TestK8sProxyTarget_PathHandling(t *testing.T) {
	tests := []struct {
		name         string
		inputPath    string
		expectedPath string
	}{
		{
			name:         "no leading slash",
			inputPath:    "metrics",
			expectedPath: "metrics",
		},
		{
			name:         "with leading slash",
			inputPath:    "/metrics",
			expectedPath: "/metrics",
		},
		{
			name:         "nested path",
			inputPath:    "api/v1/metrics",
			expectedPath: "api/v1/metrics",
		},
		{
			name:         "nested path with leading slash",
			inputPath:    "/api/v1/metrics",
			expectedPath: "/api/v1/metrics",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proxyGetter := &mockPodProxyGetter{response: "test"}
			url := "http://10.0.0.1:3001/metrics"
			target := NewK8sProxyTarget(url, proxyGetter, "test-ns", "test-pod", 3001, tt.inputPath)

			if target.path != tt.expectedPath {
				t.Errorf("Expected path '%s', got '%s'", tt.expectedPath, target.path)
			}
		})
	}
}
