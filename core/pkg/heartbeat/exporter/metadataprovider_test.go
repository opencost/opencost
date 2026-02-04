package exporter

import (
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/clusters"
)

type MockClusterInfoProvider struct{}

var (
	logLevel    = "trace"
	ClusterInfo = map[string]string{
		clusters.ClusterInfoIdKey:       "test-cluster-id",
		clusters.ClusterInfoNameKey:     "test-cluster-name",
		clusters.ClusterInfoVersionKey:  "test-cluster-version",
		clusters.ClusterInfoRegionKey:   "test-cluster-region",
		clusters.ClusterInfoProviderKey: "test-cluster-provider",
	}
)

func NewMockClusterInfoProvider() clusters.ClusterInfoProvider {
	return new(MockClusterInfoProvider)
}
func (m *MockClusterInfoProvider) GetClusterInfo() map[string]string {
	return ClusterInfo
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

	for k, v := range ClusterInfo {
		if md[k] != v {
			t.Errorf("Expected metadata key %s to be %s, got %s", k, v, md[k])
		}
	}

	if heartbeatSrc.Name() != "heartbeat-source" {
		t.Errorf("Expected source name to be 'heartbeat-source', got '%s'", heartbeatSrc.Name())
	}
}

func TestLogLevelMetadataProvider(t *testing.T) {
	t.Parallel()

	logLevelMetaDataProvider := NewLogLevelMetadataProvider()

	heartbeatSrc := NewHeartbeatSource("test-app", "v0.0.1", logLevelMetaDataProvider)

	hb := heartbeatSrc.Make(time.Now().UTC().Truncate(time.Second))

	md := hb.Metadata
	if md == nil {
		t.Errorf("Expected metadata to be non-nil, got nil")
	}

	if md["logLevel"] != logLevel {
		t.Errorf("Expected log level to be '%s', got '%s'", logLevel, md["logLevel"])
	}

	if heartbeatSrc.Name() != "heartbeat-source" {
		t.Errorf("Expected source name to be 'heartbeat-source', got '%s'", heartbeatSrc.Name())
	}
}

func TestMultiMetadataProvider(t *testing.T) {
	t.Parallel()

	provider := NewMockClusterInfoProvider()
	clusterInfoMetaDataProvider := NewClusterInfoMetadataProvider(provider)
	logLevelMetaDataProvider := NewLogLevelMetadataProvider()
	multiMetaDataProvider := NewMultiMetadataProvider(clusterInfoMetaDataProvider, logLevelMetaDataProvider)

	heartbeatSrc := NewHeartbeatSource("test-app", "v0.0.1", multiMetaDataProvider)

	hb := heartbeatSrc.Make(time.Now().UTC().Truncate(time.Second))

	md := hb.Metadata
	if md == nil {
		t.Errorf("Expected metadata to be non-nil, got nil")
	}

	for k, v := range ClusterInfo {
		if md[k] != v {
			t.Errorf("Expected metadata key %s to be %s, got %s", k, v, md[k])
		}
	}

	if md["logLevel"] != logLevel {
		t.Errorf("Expected log level to be '%s', got '%s'", logLevel, md["logLevel"])
	}

	if heartbeatSrc.Name() != "heartbeat-source" {
		t.Errorf("Expected source name to be 'heartbeat-source', got '%s'", heartbeatSrc.Name())
	}
}
