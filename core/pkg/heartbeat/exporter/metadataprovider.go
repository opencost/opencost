package exporter

import (
	"maps"

	"github.com/opencost/opencost/core/pkg/clusters"
)

// HeartbeatMetadataProvider is an interface that provides metadata for heartbeat instances. It can be used to inject
// custom metadata into a generic `Heartbeat` payload.
type HeartbeatMetadataProvider interface {
	// GetMetadata returns the metadata for new heartbeat instances.
	GetMetadata() map[string]any
}

// ClusterInfoMetadataProvider is a `HeartbeatMetadataProvider` implementation that provides metadata about the cluster
// leveraging a `ClusterInfoProvider` implementation.
type ClusterInfoMetadataProvider struct {
	clusterInfoProvider clusters.ClusterInfoProvider
}

// NewClusterInfoMetadataProvider creates a new `ClusterInfoMetadataProvider` instance. The `provider` parameter is used to
// inject custom metadata, but can be set to `nil` if no metadata is needed.
func NewClusterInfoMetadataProvider(provider clusters.ClusterInfoProvider) *ClusterInfoMetadataProvider {
	return &ClusterInfoMetadataProvider{
		clusterInfoProvider: provider,
	}
}

// GetMetadata returns the metadata for new heartbeat instances. It uses the `ClusterInfoProvider` to get the cluster
// information and injects it into the metadata map.
func (c *ClusterInfoMetadataProvider) GetMetadata() map[string]any {
	m := c.clusterInfoProvider.GetClusterInfo()
	metadata := make(map[string]any, len(m))

	for k, v := range m {
		metadata[k] = v
	}

	return metadata
}

type LogLevelMetadataProvider struct {
	logLevel string
}

func NewLogLevelMetadataProvider(logLevel string) *LogLevelMetadataProvider {
	return &LogLevelMetadataProvider{
		logLevel: logLevel,
	}
}

func (l *LogLevelMetadataProvider) GetMetadata() map[string]any {
	return map[string]any{
		"logLevel": l.logLevel,
	}
}

// MultiMetadataProvider is a `HeartbeatMetadataProvider` implementation that provides metadata from multiple providers.
type MultiMetadataProvider struct {
	providers []HeartbeatMetadataProvider
}

func NewMultiMetadataProvider(providers ...HeartbeatMetadataProvider) *MultiMetadataProvider {
	return &MultiMetadataProvider{
		providers: providers,
	}
}

func (m *MultiMetadataProvider) GetMetadata() map[string]any {
	metadata := make(map[string]any)

	for _, provider := range m.providers {
		maps.Copy(metadata, provider.GetMetadata())
	}

	return metadata
}
