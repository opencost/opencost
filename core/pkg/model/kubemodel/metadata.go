package kubemodel

import (
	"time"
)

// @bingen:generate:Metadata
type Metadata struct {
	CreatedAt       time.Time       `json:"createdAt"`             // @bingen:field[version=1]
	CompletedAt     time.Time       `json:"completedAt"`           // @bingen:field[version=1]
	ObjectCount     int             `json:"objectCount"`           // @bingen:field[version=1]
	Diagnostics     []Diagnostic    `json:"diagnostics,omitempty"` // @bingen:field[version=1]
	DiagnosticLevel DiagnosticLevel `json:"diagnosticLevel"`       // @bingen:field[version=1]
}
