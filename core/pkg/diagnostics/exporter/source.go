package exporter

import (
	"context"
	"time"

	"github.com/opencost/opencost/core/pkg/diagnostics"
)

// DiagnosticSource is an `export.ExportSource` implementation that provides the basic data for a `DiagnosticResult` payload.
type DiagnosticSource struct {
	diagnosticService diagnostics.DiagnosticService
}

// NewDiagnosticSource creates a new `HeartbeatSource` instance. The `provider` parameter is used to inject custom metadata,
// but can be set to `nil` if no metadata is needed.
func NewDiagnosticSource(diagnosticService diagnostics.DiagnosticService) *DiagnosticSource {
	return &DiagnosticSource{
		diagnosticService: diagnosticService,
	}
}

// Make creates a new `DiagnosticsRunReport` instance with the provided current time.
func (ds *DiagnosticSource) Make(t time.Time) *diagnostics.DiagnosticsRunReport {
	ctx := context.Background()

	return &diagnostics.DiagnosticsRunReport{
		StartTime: t,
		Results:   ds.diagnosticService.Run(ctx),
	}
}

func (ds *DiagnosticSource) Name() string {
	return diagnostics.DiagnosticsEventName + "-source"
}
