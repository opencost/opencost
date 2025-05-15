package exporter

import (
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/clusters"
)

type MockClusterInfoProvider struct{}

func NewMockClusterInfoProvider() clusters.ClusterInfoProvider {
	return new(MockClusterInfoProvider)
}
func (m *MockClusterInfoProvider) GetClusterInfo() map[string]string {
	return map[string]string{
		clusters.ClusterInfoIdKey:       "test-cluster-id",
		clusters.ClusterInfoNameKey:     "test-cluster-name",
		clusters.ClusterInfoVersionKey:  "test-cluster-version",
		clusters.ClusterInfoRegionKey:   "test-cluster-region",
		clusters.ClusterInfoProviderKey: "test-cluster-provider",
	}
}

func TestClusterInfoProvider(t *testing.T) {
	t.Parallel()

	provider := NewMockClusterInfoProvider()
	clusterInfoMetaDataProvider := NewClusterInfoMetadataProvider(provider)

	heartbeatSrc := NewHeartbeatSource("test-app", "v0.0.1", clusterInfoMetaDataProvider)

	hb := heartbeatSrc.Make(time.Now().UTC().Truncate(time.Second))

	md := hb.Metadata
	if md == nil {
		t.Errorf("Expected metadata to be non-nil, got nil")
	}

	if md[clusters.ClusterInfoIdKey] != "test-cluster-id" {
		t.Errorf("Expected cluster ID to be 'test-cluster-id', got '%s'", md[clusters.ClusterInfoIdKey])
	}
	if md[clusters.ClusterInfoNameKey] != "test-cluster-name" {
		t.Errorf("Expected cluster name to be 'test-cluster-name', got '%s'", md[clusters.ClusterInfoNameKey])
	}
	if md[clusters.ClusterInfoVersionKey] != "test-cluster-version" {
		t.Errorf("Expected cluster version to be 'test-cluster-version', got '%s'", md[clusters.ClusterInfoVersionKey])
	}
	if md[clusters.ClusterInfoRegionKey] != "test-cluster-region" {
		t.Errorf("Expected cluster region to be 'test-cluster-region', got '%s'", md[clusters.ClusterInfoRegionKey])
	}
	if md[clusters.ClusterInfoProviderKey] != "test-cluster-provider" {
		t.Errorf("Expected cluster provider to be 'test-cluster-provider', got '%s'", md[clusters.ClusterInfoProviderKey])
	}

	if heartbeatSrc.Name() != "heartbeat-source" {
		t.Errorf("Expected source name to be 'heartbeat-source', got '%s'", heartbeatSrc.Name())
	}
}
