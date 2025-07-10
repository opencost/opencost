package exporter

import (
	"time"

	"github.com/google/uuid"
	"github.com/opencost/opencost/core/pkg/heartbeat"
)

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
