package scrape

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/modules/collector-source/pkg/scrape/target"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// mockClusterCache implements clustercache.ClusterCache for testing
type mockClusterCache struct {
	pods []*clustercache.Pod
}

func (m *mockClusterCache) GetAllPods() []*clustercache.Pod {
	return m.pods
}

func (m *mockClusterCache) GetAllNodes() []*clustercache.Node {
	return nil
}

func (m *mockClusterCache) GetAllNamespaces() []*clustercache.Namespace {
	return nil
}

func (m *mockClusterCache) GetAllPersistentVolumes() []*clustercache.PersistentVolume {
	return nil
}

func (m *mockClusterCache) GetAllPersistentVolumeClaims() []*clustercache.PersistentVolumeClaim {
	return nil
}

func (m *mockClusterCache) GetAllStorageClasses() []*clustercache.StorageClass {
	return nil
}

func (m *mockClusterCache) GetAllServices() []*clustercache.Service {
	return nil
}

func (m *mockClusterCache) GetAllDeployments() []*clustercache.Deployment {
	return nil
}

func (m *mockClusterCache) GetAllStatefulSets() []*clustercache.StatefulSet {
	return nil
}

func (m *mockClusterCache) GetAllDaemonSets() []*clustercache.DaemonSet {
	return nil
}

func (m *mockClusterCache) GetAllJobs() []*clustercache.Job {
	return nil
}

func (m *mockClusterCache) GetAllReplicaSets() []*clustercache.ReplicaSet {
	return nil
}

func (m *mockClusterCache) GetAllPodDisruptionBudgets() []*clustercache.PodDisruptionBudget {
	return nil
}

func (m *mockClusterCache) GetAllReplicationControllers() []*clustercache.ReplicationController {
	return nil
}

func (m *mockClusterCache) GetAllResourceQuotas() []*clustercache.ResourceQuota {
	return nil
}

func (m *mockClusterCache) Run() {}

func (m *mockClusterCache) Stop() {}

// mockPodProxyGetter implements target.PodProxyGetter for testing
type mockNetworkPodProxyGetter struct{}

func (m *mockNetworkPodProxyGetter) Get(ctx context.Context, namespace, podName string, port int, path string) (io.Reader, error) {
	return strings.NewReader("test response"), nil
}

func TestIsNetworkCosts(t *testing.T) {
	tests := []struct {
		name     string
		labels   map[string]string
		expected bool
	}{
		{
			name: "valid network costs labels",
			labels: map[string]string{
				"app.kubernetes.io/name":     "network-costs",
				"app.kubernetes.io/instance": "kubecost",
			},
			expected: true,
		},
		{
			name: "missing name label",
			labels: map[string]string{
				"app.kubernetes.io/instance": "kubecost",
			},
			expected: false,
		},
		{
			name: "missing instance label",
			labels: map[string]string{
				"app.kubernetes.io/name": "network-costs",
			},
			expected: false,
		},
		{
			name: "wrong name value",
			labels: map[string]string{
				"app.kubernetes.io/name":     "wrong-name",
				"app.kubernetes.io/instance": "kubecost",
			},
			expected: false,
		},
		{
			name: "wrong instance value",
			labels: map[string]string{
				"app.kubernetes.io/name":     "network-costs",
				"app.kubernetes.io/instance": "wrong-instance",
			},
			expected: false,
		},
		{
			name:     "empty labels",
			labels:   map[string]string{},
			expected: false,
		},
		{
			name:     "nil labels",
			labels:   nil,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isNetworkCosts(tt.labels)
			if result != tt.expected {
				t.Errorf("isNetworkCosts() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

func TestNewNetworkTargetProvider(t *testing.T) {
	cache := &mockClusterCache{}
	proxyGetter := &mockNetworkPodProxyGetter{}
	port := 3001

	provider := NewNetworkTargetProvider(port, cache, proxyGetter)

	if provider == nil {
		t.Fatal("Expected non-nil provider")
	}

	if provider.port != port {
		t.Errorf("Expected port %d, got %d", port, provider.port)
	}

	if provider.clusterCache == nil {
		t.Error("Expected clusterCache to be set")
	}

	if provider.proxyGetter == nil {
		t.Error("Expected proxyGetter to be set")
	}
}

func TestNetworkTargetProvider_GetTargets_NoProxyGetter(t *testing.T) {
	// Create a pod with network-costs labels
	pod := &clustercache.Pod{
		UID:       types.UID("test-uid"),
		Name:      "network-costs-pod",
		Namespace: "opencost",
		Labels: map[string]string{
			"app.kubernetes.io/name":     "network-costs",
			"app.kubernetes.io/instance": "kubecost",
		},
		Status: clustercache.PodStatus{
			Phase: v1.PodRunning,
			PodIP: "10.0.0.1",
		},
	}

	cache := &mockClusterCache{pods: []*clustercache.Pod{pod}}
	provider := NewNetworkTargetProvider(3001, cache, nil)

	targets := provider.GetTargets()

	if len(targets) != 1 {
		t.Fatalf("Expected 1 target, got %d", len(targets))
	}

	// When no proxy getter, should use UrlTarget
	if _, ok := targets[0].(*target.UrlTarget); !ok {
		t.Errorf("Expected UrlTarget when proxyGetter is nil, got %T", targets[0])
	}
}

func TestNetworkTargetProvider_GetTargets_WithProxyGetter(t *testing.T) {
	// Create a pod with network-costs labels
	pod := &clustercache.Pod{
		UID:       types.UID("test-uid"),
		Name:      "network-costs-pod",
		Namespace: "opencost",
		Labels: map[string]string{
			"app.kubernetes.io/name":     "network-costs",
			"app.kubernetes.io/instance": "kubecost",
		},
		Status: clustercache.PodStatus{
			Phase: v1.PodRunning,
			PodIP: "10.0.0.1",
		},
	}

	cache := &mockClusterCache{pods: []*clustercache.Pod{pod}}
	proxyGetter := &mockNetworkPodProxyGetter{}
	provider := NewNetworkTargetProvider(3001, cache, proxyGetter)

	targets := provider.GetTargets()

	if len(targets) != 1 {
		t.Fatalf("Expected 1 target, got %d", len(targets))
	}

	// When proxy getter is available, should use K8sProxyTarget
	if _, ok := targets[0].(*target.K8sProxyTarget); !ok {
		t.Errorf("Expected K8sProxyTarget when proxyGetter is available, got %T", targets[0])
	}
}

func TestNetworkTargetProvider_GetTargets_FiltersPods(t *testing.T) {
	pods := []*clustercache.Pod{
		// Valid network-costs pod
		{
			UID:       types.UID("test-uid-1"),
			Name:      "network-costs-pod",
			Namespace: "opencost",
			Labels: map[string]string{
				"app.kubernetes.io/name":     "network-costs",
				"app.kubernetes.io/instance": "kubecost",
			},
			Status: clustercache.PodStatus{
				Phase: v1.PodRunning,
				PodIP: "10.0.0.1",
			},
		},
		// Pod with wrong labels
		{
			UID:       types.UID("test-uid-2"),
			Name:      "other-pod",
			Namespace: "default",
			Labels: map[string]string{
				"app": "other",
			},
			Status: clustercache.PodStatus{
				Phase: v1.PodRunning,
				PodIP: "10.0.0.2",
			},
		},
		// Pod not running
		{
			UID:       types.UID("test-uid-3"),
			Name:      "pending-pod",
			Namespace: "opencost",
			Labels: map[string]string{
				"app.kubernetes.io/name":     "network-costs",
				"app.kubernetes.io/instance": "kubecost",
			},
			Status: clustercache.PodStatus{
				Phase: v1.PodPending,
				PodIP: "10.0.0.3",
			},
		},
	}

	cache := &mockClusterCache{pods: pods}
	provider := NewNetworkTargetProvider(3001, cache, nil)

	targets := provider.GetTargets()

	// Should only return the one valid running network-costs pod
	if len(targets) != 1 {
		t.Errorf("Expected 1 target, got %d", len(targets))
	}
}

func TestNetworkTargetProvider_GetTargets_EmptyCache(t *testing.T) {
	cache := &mockClusterCache{pods: []*clustercache.Pod{}}
	provider := NewNetworkTargetProvider(3001, cache, nil)

	targets := provider.GetTargets()

	if len(targets) != 0 {
		t.Errorf("Expected 0 targets, got %d", len(targets))
	}
}

func TestNewNetworkScraper(t *testing.T) {
	cache := &mockClusterCache{}
	proxyGetter := &mockNetworkPodProxyGetter{}
	port := 3001

	scraper := newNetworkScraper(port, cache, proxyGetter)

	if scraper == nil {
		t.Fatal("Expected non-nil scraper")
	}

	// Verify it's a TargetScraper
	if _, ok := scraper.(*TargetScraper); !ok {
		t.Errorf("Expected TargetScraper, got %T", scraper)
	}
}

func TestNewNetworkTargetScraper(t *testing.T) {
	cache := &mockClusterCache{}
	provider := NewNetworkTargetProvider(3001, cache, nil)

	scraper := newNetworkTargetScraper(provider)

	if scraper == nil {
		t.Fatal("Expected non-nil scraper")
	}

	// Verify it's a TargetScraper
	if _, ok := interface{}(scraper).(*TargetScraper); !ok {
		t.Errorf("Expected TargetScraper, got %T", scraper)
	}
}
