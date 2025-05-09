package exporter

import (
	"time"

	"github.com/google/uuid"
	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/core/pkg/heartbeat"
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

// HeartbeatSource is an `export.ExportSource` implementation that provides the basic data for a `Heartbeat` payload, and
// leverages a `HeartbeatMetadataProvider` to inject custom metadata.
type HeartbeatSource struct {
	startTime        time.Time
	applicationName  string
	version          string
	metadataProvider HeartbeatMetadataProvider
}

// NewHeartbeatSource creates a new `HeartbeatSource` instance. The `provider` parameter is used to inject custom metadata,
// but can be set to `nil` if no metadata is needed.
func NewHeartbeatSource(applicationName string, version string, provider HeartbeatMetadataProvider) *HeartbeatSource {
	return &HeartbeatSource{
		startTime:        time.Now().UTC(),
		applicationName:  applicationName,
		version:          version,
		metadataProvider: provider,
	}
}

// Make creates a new `Heartbeat` instance with the provided current time.
func (h *HeartbeatSource) Make(t time.Time) *heartbeat.Heartbeat {
	id := uuid.Must(uuid.NewV7()).String()
	uptime := uint64(t.Sub(h.startTime).Minutes())

	var metadata map[string]any
	if h.metadataProvider != nil {
		metadata = h.metadataProvider.GetMetadata()
	}

	return heartbeat.NewHeartbeat(id, t, uptime, h.applicationName, h.version, metadata)
}

func (h *HeartbeatSource) Name() string {
	return heartbeat.HeartbeatEventName + "-source"
}
