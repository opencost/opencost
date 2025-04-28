package heartbeat

import (
	"github.com/opencost/opencost/core/pkg/exporter"
	"github.com/opencost/opencost/core/pkg/storage"
)

// NewHeartbeatExportController creates a new EventExportController for Heartbeat events.
// A HeartbeatMetadataProvider can optionally be provided to append metadata to the Heartbeat payload.
func NewHeartbeatExportController(clusterId string, store storage.Storage, provider HeartbeatMetadataProvider) *exporter.EventExportController[Heartbeat] {
	return exporter.NewEventExportController(
		NewHeartbeatSource(provider),
		NewHeartbeatExporter(clusterId, store),
	)
}
