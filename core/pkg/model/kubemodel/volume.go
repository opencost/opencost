package kubemodel

import (
	"time"
)

// Volume represents a Kubernetes volume
type Volume struct {
	ID           string            `json:"id"`
	ClusterID    string            `json:"clusterId"`
	Name         string            `json:"name"`
	Namespace    string            `json:"namespace"`
	Labels       map[string]string `json:"labels,omitempty"`
	Annotations  map[string]string `json:"annotations,omitempty"`
	CreationTime *time.Time        `json:"creationTime,omitempty"`
	DeletionTime *time.Time        `json:"deletionTime,omitempty"`
	StorageClass string            `json:"storageClass"`
	Size         uint64            `json:"size"`
	Cost         float64           `json:"cost"`
	Diagnostic   *DiagnosticResult `json:"diagnostic,omitempty"`
}