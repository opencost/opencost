package kubemodel

import (
	"time"
)

// Namespace represents a Kubernetes namespace
type Namespace struct {
	ID           string            `json:"id"`
	ClusterID    string            `json:"clusterId"`
	Name         string            `json:"name"`
	Labels       map[string]string `json:"labels,omitempty"`
	Annotations  map[string]string `json:"annotations,omitempty"`
	Start        *time.Time        `json:"start,omitempty"`
	End          *time.Time        `json:"end,omitempty"`
	Diagnostic   *DiagnosticResult `json:"diagnostic,omitempty"`
}