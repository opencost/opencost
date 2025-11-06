package kubemodel

import (
	"time"
)

// Namespace represents a Kubernetes namespace
type Namespace struct {
	ID             string                     `json:"id"`
	ClusterID      string                     `json:"clusterId"`
	Name           string                     `json:"name"`
	Labels         map[string]string          `json:"labels,omitempty"`
	Annotations    map[string]string          `json:"annotations,omitempty"`
	Start          time.Time                  `json:"start"`
	End            time.Time                  `json:"end"`
	Controllers    map[string]*Controller     `json:"controllers"`
	ResourceQuotas map[string]*ResourceQuota  `json:"resourceQuotas,omitempty"`
	Diagnostic     *DiagnosticResult          `json:"diagnostic,omitempty"`
}