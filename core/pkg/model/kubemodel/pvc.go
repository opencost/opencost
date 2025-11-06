package kubemodel

import (
	"time"
)

// PersistentVolumeClaim represents a Kubernetes persistent volume claim
type PersistentVolumeClaim struct {
	ID                 string            `json:"id"`                   // @bingen:field[version=1]
	NamespaceID        string            `json:"namespaceId"`          // @bingen:field[version=1]
	VolumeID           *string           `json:"volumeId,omitempty"`   // @bingen:field[version=1]
	PodID              *string           `json:"podId,omitempty"`      // @bingen:field[version=1]
	Name               string            `json:"name"`                 // @bingen:field[version=1]
	Labels             map[string]string `json:"labels,omitempty"`     // @bingen:field[version=1]
	Annotations        map[string]string `json:"annotations,omitempty"` // @bingen:field[version=1]
	Start              time.Time         `json:"start"`                // @bingen:field[version=1]
	End                time.Time         `json:"end"`                  // @bingen:field[version=1]
	StorageClass       string            `json:"storageClass"`         // @bingen:field[version=1]
	StorageByteSeconds uint64            `json:"storageByteSeconds"`   // @bingen:field[version=1]
	RequestedBytes     uint64            `json:"requestedBytes"`       // @bingen:field[version=1]
	Size               uint64            `json:"size"`                 // @bingen:field[version=1]
	VolumeName         string            `json:"volumeName"`           // @bingen:field[version=1]
	Diagnostic         *DiagnosticResult `json:"diagnostic,omitempty"` // @bingen:field[version=1]
}