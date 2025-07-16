package exporter

import (
	"github.com/opencost/opencost/core/pkg/diagnostics"
	"github.com/opencost/opencost/core/pkg/exporter"
	"github.com/opencost/opencost/core/pkg/exporter/pathing"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/storage"
)

// NewDiagnosticExporter creates a new `StorageExporter[DiagnosticsRunReport]` instance for exporting diagnostic run events.
func NewDiagnosticExporter(clusterId string, applicationName string, storage storage.Storage) exporter.EventExporter[diagnostics.DiagnosticsRunReport] {
	pathing, err := pathing.NewEventStoragePathFormatter(applicationName, clusterId, diagnostics.DiagnosticsEventName)
	if err != nil {
		log.Errorf("failed to create pathing formatter: %v", err)
		return nil
	}

	return exporter.NewEventStorageExporter(
		pathing,
		NewDiagnosticsEncoder(),
		storage,
	)
}
