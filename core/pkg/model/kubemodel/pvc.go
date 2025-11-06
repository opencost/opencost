package kubemodel

import (
	"time"
)

// PersistentVolumeClaim represents a Kubernetes persistent volume claim
type PersistentVolumeClaim struct {
	ID               string            `json:"id"`
	NamespaceID      string            `json:"namespaceId"`
	VolumeID         *string           `json:"volumeId,omitempty"`
	PodID            *string           `json:"podId,omitempty"`
	Name             string            `json:"name"`
	Labels           map[string]string `json:"labels,omitempty"`
	Annotations      map[string]string `json:"annotations,omitempty"`
	CreationTime     *time.Time        `json:"creationTime,omitempty"`
	DeletionTime     *time.Time        `json:"deletionTime,omitempty"`
	StorageClass     string            `json:"storageClass"`
	StorageByteHours uint64            `json:"storageByteHours"`
	RequestedBytes   uint64            `json:"requestedBytes"`
	Size             uint64            `json:"size"`
	VolumeName       string            `json:"volumeName"`
	Diagnostic       *DiagnosticResult `json:"diagnostic,omitempty"`
}