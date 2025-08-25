package diagnostics

import (
	"context"
	"time"
)

// DiagnosticsEventName is used to represent the name of the diagnostics export pipeline event to categorize for storage.
const DiagnosticsEventName string = "diagnostics"

// DiagnosticResult represent the result of a diagnostic run, and contains basic diagnostic information and additional
// custom diagnostic information appended by the specific runner.
type DiagnosticResult struct {
	// Unique Identifier for the diagnostic run result.
	ID string `json:"id"`

	// Name of the diagnostic that ran.
	Name string `json:"name"`

	// Description of the diagnostic run, human readable description of what the diagnostic shows.
	Description string `json:"description"`

	// Category of the diagnostic run, which can be used to group similar diagnostics together.
	Category string `json:"category"`

	// Timestamp containing the time when the diagnostic run was executed.
	Timestamp time.Time `json:"timestamp"`

	// Error message if the diagnostic run failed. If this field is non-empty, the diagnostic run should be
	// considered a failure.
	Error string `json:"error,omitempty"`

	// Details contains additional custom information about the diagnostic run that can be added by the diagnostic
	// runner.
	Details map[string]any `json:"details,omitempty"`
}

// DiagnosticsRunReport is a struct that contains the start time of the diagnostics run, and all of the results.
type DiagnosticsRunReport struct {
	// Application contains the name of the application that the diagnostics run belongs to.
	Application string `json:"application"`

	// StartTime contains the time when the full diagnostics run started
	StartTime time.Time `json:"startTime"`

	// Results contains all of the results of the diagnostics run.
	Results []*DiagnosticResult `json:"results"`
}

// DiagnosticRunner is a function that executes a diagnostic and returns the result. The function should return a map containing
// any additional information about the diagnostic run, and a detailed error if the run failed.
type DiagnosticRunner func(context.Context) (map[string]any, error)

// Diagnostic is a struct that contains the basic information about a registed diagnostic within a DiagnosticService.
type Diagnostic struct {
	// Name of the diagnostic that is registered.
	Name string

	// Description of the diagnostic that is registered.
	Description string

	// Category of the diagnostic that is registered.
	Category string
}

// DiagnosticService is an interface that defines the basic contract for a service that registers and runs diagnostics on demand and provides
// the results.
type DiagnosticService interface {
	// Register registers a new diagnostic runner implementation with the service that will run the next time diagnostics are requested.
	// An error is returned if a runner failed to register. Note that category _and_ name must be a unique combination.
	Register(name, description, category string, runner DiagnosticRunner) error

	// Unregister unregisters a diagnostic runner implementation with the service. True is returned if the runner was unregistered successfully,
	// false otherwise.
	Unregister(name, category string) bool

	// Run executes all registered diagnostics and returns the results.
	Run(ctx context.Context) []*DiagnosticResult

	// RunCategory executes all registered diagnostics in the provided category.
	RunCategory(ctx context.Context, category string) []*DiagnosticResult

	// RunDiagnostic executes a specific diagnostic by category and name. If the diagnostic does not exist, nil is returned.
	RunDiagnostic(ctx context.Context, category, name string) *DiagnosticResult

	// Diagnostics returns a list of all registered diagnostics.
	Diagnostics() []Diagnostic

	// Total returns the total number of registered diagnostics.
	Total() int
}
