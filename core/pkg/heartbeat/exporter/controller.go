package exporter

import (
	"github.com/opencost/opencost/core/pkg/exporter"
	"github.com/opencost/opencost/core/pkg/heartbeat"
	"github.com/opencost/opencost/core/pkg/storage"
)

// NewHeartbeatExportController creates a new EventExportController for Heartbeat events.
// A HeartbeatMetadataProvider can optionally be provided to append metadata to the Heartbeat payload.
func NewHeartbeatExportController(
	clusterId string,
	applicationName string,
	version string,
	store storage.Storage,
	provider HeartbeatMetadataProvider,
) *exporter.EventExportController[heartbeat.Heartbeat] {
	return exporter.NewEventExportController(
		NewHeartbeatSource(applicationName, version, provider),
		NewHeartbeatExporter(clusterId, applicationName, store),
	)
}
