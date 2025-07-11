package exporter

import (
	"github.com/opencost/opencost/core/pkg/diagnostics"
	"github.com/opencost/opencost/core/pkg/exporter"
)

// NewDiagnosticsEncoder returns a JSON encoder used to encode DiagnosticsRunReport events.
func NewDiagnosticsEncoder() exporter.Encoder[diagnostics.DiagnosticsRunReport] {
	return exporter.NewGZipEncoder(exporter.NewJSONEncoder[diagnostics.DiagnosticsRunReport]())
}
