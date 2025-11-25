package kubemodel

import "time"

// @bingen:generate:PersistentVolumeClaim
type PersistentVolumeClaim struct {
	UID                         string            `json:"uid"`                         // @bingen:field[version=1]
	NamespaceUID                string            `json:"namespaceUid"`                // @bingen:field[version=1]
	VolumeUID                   *string           `json:"volumeUid,omitempty"`         // @bingen:field[version=1]
	PodUID                      *string           `json:"podUid,omitempty"`            // @bingen:field[version=1]
	Name                        string            `json:"name"`                        // @bingen:field[version=1]
	Labels                      map[string]string `json:"labels,omitempty"`            // @bingen:field[version=1]
	Annotations                 map[string]string `json:"annotations,omitempty"`       // @bingen:field[version=1]
	Start                       time.Time         `json:"start"`                       // @bingen:field[version=1]
	End                         time.Time         `json:"end"`                         // @bingen:field[version=1]
	StorageClass                string            `json:"storageClass"`                // @bingen:field[version=1]
	StorageByteSecondsAllocated uint64            `json:"storageByteSecondsAllocated"` // @bingen:field[version=1]
	RequestedBytesAllocated     uint64            `json:"requestedBytesAllocated"`     // @bingen:field[version=1]
	Size                        uint64            `json:"size"`                        // @bingen:field[version=1]
	VolumeName                  string            `json:"volumeName"`                  // @bingen:field[version=1]
}
