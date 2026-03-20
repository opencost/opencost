package exporter

import (
	"maps"

	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/core/pkg/log"
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

// LogLevelMetadataProvider is a `HeartbeatMetadataProvider` implementation that provides the log level.
type LogLevelMetadataProvider struct{}

// NewLogLevelMetadataProvider creates a new `LogLevelMetadataProvider` instance.
func NewLogLevelMetadataProvider() *LogLevelMetadataProvider {
	return &LogLevelMetadataProvider{}
}

// GetMetadata returns the metadata for new heartbeat instances. It uses the log level from the global logger.
func (l *LogLevelMetadataProvider) GetMetadata() map[string]any {
	return map[string]any{
		"logLevel": log.GetLogLevel(),
	}
}

// MultiMetadataProvider is a `HeartbeatMetadataProvider` implementation that provides metadata from multiple providers.
type MultiMetadataProvider struct {
	providers []HeartbeatMetadataProvider
}

// NewMultiMetadataProvider creates a new `MultiMetadataProvider` instance.
func NewMultiMetadataProvider(providers ...HeartbeatMetadataProvider) *MultiMetadataProvider {
	return &MultiMetadataProvider{
		providers: providers,
	}
}

// GetMetadata returns the metadata for new heartbeat instances.
// It uses the `MultiMetadataProvider` to get the metadata from multiple providers and injects it into the metadata map.
func (m *MultiMetadataProvider) GetMetadata() map[string]any {
	metadata := make(map[string]any)

	for _, provider := range m.providers {
		maps.Copy(metadata, provider.GetMetadata())
	}

	return metadata
}
