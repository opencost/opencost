package exporter

import (
	"github.com/opencost/opencost/core/pkg/diagnostics"
	"github.com/opencost/opencost/core/pkg/exporter"
	"github.com/opencost/opencost/core/pkg/storage"
)

// NewDiagnosticsExportController creates a new EventExportController for DiagnosticsRunReport events.
func NewDiagnosticsExportController(
	clusterId string,
	applicationName string,
	store storage.Storage,
	service diagnostics.DiagnosticService,
) *exporter.EventExportController[diagnostics.DiagnosticsRunReport] {
	return exporter.NewEventExportController(
		NewDiagnosticSource(applicationName, service),
		NewDiagnosticExporter(clusterId, applicationName, store),
	)
}
