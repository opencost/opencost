package diagnostics

import (
	"github.com/opencost/opencost/core/pkg/filter/fieldstrings"
)

// DiagnosticsField and DiagnosticsSummaryField are enums that represent Diagnostics-specific fields that can be
// filtered on (cluster for diagnostics	, cluster, provider, region, agent version for summary)
type DiagnosticsField string
type DiagnosticsSummaryField string

// If you add a DiagnosticsField, make sure to update field maps to return the correct
// Diagnostic value does not enforce exhaustive pattern matching on "enum" types.
const (
	FieldClusterID DiagnosticsField = DiagnosticsField(fieldstrings.FieldClusterID)
)

// Field used for Diagnostics Summary filtering
const (
	FieldSummaryClusterID DiagnosticsSummaryField = DiagnosticsSummaryField(fieldstrings.FieldClusterID)
	FieldSummaryProvider  DiagnosticsSummaryField = DiagnosticsSummaryField(fieldstrings.FieldProvider)
	FieldSummaryRegion    DiagnosticsSummaryField = DiagnosticsSummaryField(fieldstrings.FieldRegion)
	FieldSummaryVersion   DiagnosticsSummaryField = DiagnosticsSummaryField(fieldstrings.FieldVersion)
)
