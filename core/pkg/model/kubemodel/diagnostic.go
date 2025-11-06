package kubemodel

import (
	"time"
)

// DiagnosticResult represents the result of a diagnostic run
type DiagnosticResult struct {
	// Unique Identifier for the diagnostic run result
	ID string `json:"id"`

	// Name of the diagnostic that ran
	Name string `json:"name"`

	// Description of the diagnostic run, human readable description
	Description string `json:"description"`

	// Category of the diagnostic run, used to group similar diagnostics
	Category string `json:"category"`

	// Timestamp when the diagnostic run was executed
	Timestamp time.Time `json:"timestamp"`

	// Error message if the diagnostic run failed
	Error string `json:"error,omitempty"`

	// Details contains additional custom information about the diagnostic run
	Details map[string]any `json:"details,omitempty"`
}