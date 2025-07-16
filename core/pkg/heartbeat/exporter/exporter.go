package exporter

import (
	"github.com/opencost/opencost/core/pkg/exporter"
	"github.com/opencost/opencost/core/pkg/exporter/pathing"
	"github.com/opencost/opencost/core/pkg/heartbeat"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/storage"
)

// NewHeartbeatExporter creates a new `StorageExporter[Heartbeat]` instance for exporting Heartbeat events.
func NewHeartbeatExporter(clusterId string, applicationName string, storage storage.Storage) exporter.EventExporter[heartbeat.Heartbeat] {
	pathing, err := pathing.NewEventStoragePathFormatter(applicationName, clusterId, heartbeat.HeartbeatEventName)
	if err != nil {
		log.Errorf("failed to create pathing formatter: %v", err)
		return nil
	}

	return exporter.NewEventStorageExporter(
		pathing,
		NewHeartbeatEncoder(),
		storage,
	)
}
