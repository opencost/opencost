package exporter

import (
	"github.com/opencost/opencost/core/pkg/exporter"
	"github.com/opencost/opencost/core/pkg/exporter/pathing"
	"github.com/opencost/opencost/core/pkg/heartbeat"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/storage"
)

// NewHeartbeatExporter creates a new `StorageExporter[Heartbeat]` instance for exporting Heartbeat events.
func NewHeartbeatExporter(clusterId string, storage storage.Storage) *exporter.StorageExporter[heartbeat.Heartbeat] {
	pathing, err := pathing.NewEventStoragePathFormatter("", clusterId, heartbeat.HeartbeatEventName)
	if err != nil {
		log.Errorf("failed to create pathing formatter: %v", err)
		return nil
	}

	return exporter.NewStorageExporter(
		heartbeat.HeartbeatEventName,
		pathing,
		NewHeartbeatEncoder(),
		storage,
	)
}
