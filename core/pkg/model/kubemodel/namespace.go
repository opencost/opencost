package kubemodel

import (
	"time"
)

// Namespace represents a Kubernetes namespace
type Namespace struct {
	ID             string                    `json:"id"`                       // @bingen:field[version=1]
	ClusterID      string                    `json:"clusterId"`                // @bingen:field[version=1]
	Name           string                    `json:"name"`                     // @bingen:field[version=1]
	Labels         map[string]string         `json:"labels,omitempty"`         // @bingen:field[version=1]
	Annotations    map[string]string         `json:"annotations,omitempty"`    // @bingen:field[version=1]
	Start          time.Time                 `json:"start"`                    // @bingen:field[version=1]
	End            time.Time                 `json:"end"`                      // @bingen:field[version=1]
	Controllers    map[string]*Controller    `json:"controllers"`              // @bingen:field[version=1]
	ResourceQuotas map[string]*ResourceQuota `json:"resourceQuotas,omitempty"` // @bingen:field[version=1]
	Diagnostic     *DiagnosticResult         `json:"diagnostic,omitempty"`     // @bingen:field[version=1]
}