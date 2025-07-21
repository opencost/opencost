package diagnostics

import (
	"github.com/opencost/opencost/core/pkg/filter/fieldstrings"
)

// DiagnosticsField is an enum that represents Diagnostics-specific fields that can be
// filtered on (cluster)
type DiagnosticsField string

// If you add a DiagnosticsField, make sure to update field maps to return the correct
// Diagnostic value does not enforce exhaustive pattern matching on "enum" types.
const (
	FieldClusterID  DiagnosticsField = DiagnosticsField(fieldstrings.FieldClusterID)
)
