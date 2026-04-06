package target

import (
	"testing"

	"k8s.io/client-go/kubernetes/fake"
)

func TestK8sProxyTarget_Load(t *testing.T) {
	// Create a fake clientset
	fakeClient := fake.NewSimpleClientset()

	// Create a K8sProxyTarget
	target := NewK8sProxyTarget(
		"http://10.0.0.1:3001/metrics",
		fakeClient,
		"test-namespace",
		"test-pod",
		3001,
		"metrics",
	)

	// Verify the target was created correctly
	if target == nil {
		t.Fatal("Expected non-nil target")
	}

	if target.namespace != "test-namespace" {
		t.Errorf("Expected namespace 'test-namespace', got '%s'", target.namespace)
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

	if target.directTarget == nil {
		t.Error("Expected non-nil directTarget")
	}
}

func TestK8sProxyTarget_LoadWithLeadingSlash(t *testing.T) {
	// Create a fake clientset
	fakeClient := fake.NewSimpleClientset()

	// Create a K8sProxyTarget with a path that has a leading slash
	target := NewK8sProxyTarget(
		"http://10.0.0.1:3001/metrics",
		fakeClient,
		"test-namespace",
		"test-pod",
		3001,
		"/metrics",
	)

	// The path should be stored as-is
	if target.path != "/metrics" {
		t.Errorf("Expected path '/metrics', got '%s'", target.path)
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
			fakeClient := fake.NewSimpleClientset()
			url := "http://10.0.0.1:3001/metrics"
			target := NewK8sProxyTarget(url, fakeClient, tt.namespace, tt.podName, tt.port, tt.path)

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

			if target.clientset == nil {
				t.Error("Expected non-nil clientset")
			}

			if target.directTarget == nil {
				t.Error("Expected non-nil directTarget")
			}
		})
	}
}

// TestK8sProxyTarget_ImplementsScrapeTarget verifies K8sProxyTarget implements ScrapeTarget
func TestK8sProxyTarget_ImplementsScrapeTarget(t *testing.T) {
	fakeClient := fake.NewSimpleClientset()
	var _ ScrapeTarget = NewK8sProxyTarget("http://10.0.0.1:3001/metrics", fakeClient, "test-ns", "test-pod", 3001, "metrics")
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
			fakeClient := fake.NewSimpleClientset()
			url := "http://10.0.0.1:3001/metrics"
			target := NewK8sProxyTarget(url, fakeClient, "test-ns", "test-pod", 3001, tt.inputPath)

			if target.path != tt.expectedPath {
				t.Errorf("Expected path '%s', got '%s'", tt.expectedPath, target.path)
			}
		})
	}
}
