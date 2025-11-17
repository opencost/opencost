package kubemodel

import "time"

// @bingen:generate:Volume
type Volume struct {
	UID          string            `json:"uid"`                  // @bingen:field[version=1]
	ClusterUID   string            `json:"clusterUid"`           // @bingen:field[version=1]
	Name         string            `json:"name"`                 // @bingen:field[version=1]
	Namespace    string            `json:"namespace"`            // @bingen:field[version=1]
	Labels       map[string]string `json:"labels,omitempty"`     // @bingen:field[version=1]
	Annotations  map[string]string `json:"annotations,omitempty"` // @bingen:field[version=1]
	Start        time.Time         `json:"start"`                // @bingen:field[version=1]
	End          time.Time         `json:"end"`                  // @bingen:field[version=1]
	StorageClass string            `json:"storageClass"`         // @bingen:field[version=1]
	Size         uint64            `json:"size"`                 // @bingen:field[version=1]
	Cost         float64           `json:"cost"`                 // @bingen:field[version=1]
	Diagnostic   *DiagnosticResult `json:"diagnostic,omitempty"` // @bingen:field[version=1]
}