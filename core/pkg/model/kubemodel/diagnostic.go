package kubemodel

import (
	"time"
)

// DiagnosticResult represents the result of a diagnostic run
type DiagnosticResult struct {
	// Unique Identifier for the diagnostic run result
	ID string `json:"id"` // @bingen:field[version=1]

	// Name of the diagnostic that ran
	Name string `json:"name"` // @bingen:field[version=1]

	// Description of the diagnostic run, human readable description
	Description string `json:"description"` // @bingen:field[version=1]

	// Category of the diagnostic run, used to group similar diagnostics
	Category string `json:"category"` // @bingen:field[version=1]

	// Timestamp when the diagnostic run was executed
	Timestamp time.Time `json:"timestamp"` // @bingen:field[version=1]

	// Error message if the diagnostic run failed
	Error string `json:"error,omitempty"` // @bingen:field[version=1]

	// Details contains additional custom information about the diagnostic run
	Details map[string]any `json:"details,omitempty"` // @bingen:field[version=1]
}