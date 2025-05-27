package test

import (
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/core/pkg/util/promutil"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/clustercache"
	"github.com/opencost/opencost/pkg/costmodel"
	metav1 "k8s.io/apimachinery/pkg/version"
	"k8s.io/client-go/discovery"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
)

func TestClusterInfoLabels(t *testing.T) {
	expected := map[string]bool{"clusterprofile": true, "errorreporting": true, "id": true, "logcollection": true, "name": true, "productanalytics": true, "provider": true, "provisioner": true, "remotereadenabled": true, "thanosenabled": true, "valuesreporting": true, "version": true}
	clusterInfo := `{"clusterProfile":"production","errorReporting":"true","id":"cluster-one","logCollection":"true","name":"bolt-3","productAnalytics":"true","provider":"GCP","provisioner":"GKE","remoteReadEnabled":"false","thanosEnabled":"false","valuesReporting":"true","version":"1.14+"}`

	var m map[string]interface{}
	err := json.Unmarshal([]byte(clusterInfo), &m)
	if err != nil {
		t.Errorf("Error: %s", err)
		return
	}

	labels := promutil.MapToLabels(m)
	for k := range expected {
		if _, ok := labels[k]; !ok {
			t.Errorf("Failed to locate key: \"%s\" in labels.", k)
			return
		}
	}
}

func TestWriteReportingFlags(t *testing.T) {
	clusterInfo := make(map[string]string)
	costmodel.WriteReportingFlags(clusterInfo)

	expectedKeys := []string{
		clusters.ClusterInfoLogCollectionKey,
		clusters.ClusterInfoProductAnalyticsKey,
		clusters.ClusterInfoErrorReportingKey,
		clusters.ClusterInfoValuesReportingKey,
	}

	for _, key := range expectedKeys {
		if _, ok := clusterInfo[key]; !ok {
			t.Errorf("Missing key: %s", key)
		}
	}
}

func TestWriteClusterProfile(t *testing.T) {
	clusterInfo := make(map[string]string)
	costmodel.WriteClusterProfile(clusterInfo)

	if _, ok := clusterInfo[clusters.ClusterInfoProfileKey]; !ok {
		t.Errorf("Expected profile key %s to be present", clusters.ClusterInfoProfileKey)
	}
}

func TestWriteThanosFlags(t *testing.T) {
	clusterInfo := make(map[string]string)
	costmodel.WriteThanosFlags(clusterInfo)

	expectedKeys := []string{
		"thanosEnabled",
		"remoteReadEnabled",
	}

	for _, key := range expectedKeys {
		if _, ok := clusterInfo[key]; !ok {
			t.Errorf("Missing key: %s", key)
		}
	}
}

type MockCloudProvider struct{}

// Required methods (all no-op implementations)
func (m *MockCloudProvider) ClusterInfo() (map[string]string, error) {
	return map[string]string{"testKey": "testValue"}, nil
}

func (m *MockCloudProvider) GetAddresses() ([]byte, error) {
	return []byte{}, nil
}

func (m *MockCloudProvider) GetDisks() ([]byte, error) {
	return []byte{}, nil
}

func (m *MockCloudProvider) GetOrphanedResources() ([]models.OrphanedResource, error) {
	return []models.OrphanedResource{}, nil
}

func (m *MockCloudProvider) NodePricing(key models.Key) (*models.Node, models.PricingMetadata, error) {
	return &models.Node{}, models.PricingMetadata{}, nil
}

func (m *MockCloudProvider) GpuPricing(labels map[string]string) (string, error) {
	return "", nil
}

func (m *MockCloudProvider) PVPricing(key models.PVKey) (*models.PV, error) {
	return &models.PV{}, nil
}

func (m *MockCloudProvider) NetworkPricing() (*models.Network, error) {
	return &models.Network{}, nil
}

func (m *MockCloudProvider) LoadBalancerPricing() (*models.LoadBalancer, error) {
	return &models.LoadBalancer{}, nil
}

func (m *MockCloudProvider) AllNodePricing() (interface{}, error) {
	return map[string]*models.Node{}, nil
}

func (m *MockCloudProvider) DownloadPricingData() error {
	return nil
}

// MockKey implements models.Key interface
type MockKey struct{}

func (k MockKey) ID() string                { return "mock-id" }
func (k MockKey) Features() string          { return "mock-features" }
func (k MockKey) GPUType() string           { return "mock-gpu" }
func (k MockKey) GPUCount() int             { return 0 }
func (k MockKey) Provider() string          { return "mock-provider" }
func (k MockKey) Region() string            { return "mock-region" }
func (k MockKey) Zone() string              { return "mock-zone" }
func (k MockKey) Architecture() string      { return "mock-arch" }
func (k MockKey) OperatingSystem() string   { return "mock-os" }
func (k MockKey) StorageClass() string      { return "mock-storage-class" }
func (k MockKey) Labels() map[string]string { return map[string]string{} }

// MockPVKey implements models.PVKey interface
type MockPVKey struct{}

func (k MockPVKey) Features() string        { return "mock-pv-features" }
func (k MockPVKey) ID() string              { return "mock-pv-id" }
func (k MockPVKey) GetStorageClass() string { return "mock-pv-storage-class" }
func (k MockPVKey) GetRegion() string       { return "mock-pv-region" }

func (m *MockCloudProvider) GetKey(labels map[string]string, node *clustercache.Node) models.Key {
	return MockKey{}
}

func (m *MockCloudProvider) GetPVKey(pv *clustercache.PersistentVolume, labels map[string]string, defaultRegion string) models.PVKey {
	return MockPVKey{}
}

func (m *MockCloudProvider) UpdateConfig(r io.Reader, updateType string) (*models.CustomPricing, error) {
	return &models.CustomPricing{}, nil
}

func (m *MockCloudProvider) UpdateConfigFromConfigMap(config map[string]string) (*models.CustomPricing, error) {
	return &models.CustomPricing{}, nil
}

func (m *MockCloudProvider) GetConfig() (*models.CustomPricing, error) {
	return &models.CustomPricing{}, nil
}

func (m *MockCloudProvider) GetManagementPlatform() (string, error) {
	return "mock-platform", nil
}

func (m *MockCloudProvider) GetLocalStorageQuery(start, end time.Duration, withBreakdown, isCumulative bool) string {
	return "mock-query"
}

func (m *MockCloudProvider) ApplyReservedInstancePricing(nodes map[string]*models.Node) {
	// No-op
}

func (m *MockCloudProvider) ServiceAccountStatus() *models.ServiceAccountStatus {
	return &models.ServiceAccountStatus{}
}

func (m *MockCloudProvider) PricingSourceStatus() map[string]*models.PricingSource {
	return map[string]*models.PricingSource{}
}

func (m *MockCloudProvider) ClusterManagementPricing() (string, float64, error) {
	return "mock", 0.0, nil
}

func (m *MockCloudProvider) CombinedDiscountForNode(instanceType string, isPreemptible bool, defaultDiscount, negotiatedDiscount float64) float64 {
	return 0.0
}

func (m *MockCloudProvider) Regions() []string {
	return []string{"mock-region"}
}

func (m *MockCloudProvider) PricingSourceSummary() interface{} {
	return "mock-summary"
}

// Compile-time interface verification
var _ models.Provider = (*MockCloudProvider)(nil)

// --- Mock Discovery for ServerVersion ---
type MockDiscovery struct {
	discovery.DiscoveryInterface
	version *metav1.Info
	err     error
}

func (m *MockDiscovery) ServerVersion() (*metav1.Info, error) {
	return m.version, m.err
}

// --- Mock Kubernetes Client implementing kubernetes.Interface ---
type MockKubeClient struct {
	*kubernetesfake.Clientset
	mockDiscovery discovery.DiscoveryInterface
}

func (m *MockKubeClient) Discovery() discovery.DiscoveryInterface {
	return m.mockDiscovery
}

// --- Test function ---
func TestLocalClusterInfoProvider_GetClusterInfo(t *testing.T) {
	mockProvider := &MockCloudProvider{}
	mockVersion := &metav1.Info{Major: "1", Minor: "20"}
	mockClient := &MockKubeClient{
		Clientset:     kubernetesfake.NewSimpleClientset(),
		mockDiscovery: &MockDiscovery{version: mockVersion},
	}

	// Mock environment variables
	os.Setenv("LOG_COLLECTION_ENABLED", "true")
	os.Setenv("CLUSTER_PROFILE", "test-profile")
	defer os.Unsetenv("LOG_COLLECTION_ENABLED")
	defer os.Unsetenv("CLUSTER_PROFILE")

	local := costmodel.NewLocalClusterInfoProvider(mockClient, mockProvider)
	info := local.GetClusterInfo()

	// Check cloud provider info
	if val, ok := info["testKey"]; !ok || val != "testValue" {
		t.Errorf("Expected testKey=testValue, got: %v", val)
	}

	// Check Kubernetes version
	if version, ok := info[clusters.ClusterInfoVersionKey]; !ok || version != "1.20" {
		t.Errorf("Expected version 1.20, got: %s", version)
	}

	// Check cluster profile
	if profile, ok := info[clusters.ClusterInfoProfileKey]; !ok || profile != "test-profile" {
		t.Errorf("Expected ClusterInfoProfileKey=test-profile, got: %v", profile)
	}

	// Check reporting flags
	if val, ok := info[clusters.ClusterInfoLogCollectionKey]; !ok || val != "true" {
		t.Errorf("Expected ClusterInfoLogCollectionKey=true, got: %v", val)
	}

	// Simulate ServerVersion error
	mockClient.mockDiscovery = &MockDiscovery{err: errors.New("version error")}
	info2 := local.GetClusterInfo()
	if _, ok := info2[clusters.ClusterInfoVersionKey]; ok {
		t.Errorf("Did not expect version key when ServerVersion returns error")
	}
}
