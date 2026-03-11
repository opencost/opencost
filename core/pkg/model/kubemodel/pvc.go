package kubemodel

import (
	"fmt"
	"time"
)

// @bingen:generate:PersistentVolumeClaim
type PersistentVolumeClaim struct {
	UID                 string    `json:"uid"`
	NamespaceUID        string    `json:"namespaceUid"`
	Name                string    `json:"name"`
	PersistentVolumeUID string    `json:"persistentVolumeUID,omitempty"`
	StorageClass        string    `json:"storageClass"`
	Start               time.Time `json:"start"`
	End                 time.Time `json:"end"`
	RequestedBytes      float64   `json:"requestedBytes"`
	UsageBytesAvg       float64   `json:"usageBytesAvg"`
	UsageBytesMax       float64   `json:"usageBytesMax"`
}

func (kms *KubeModelSet) RegisterPVC(pvc *PersistentVolumeClaim) error {
	if pvc.UID == "" {
		err := fmt.Errorf("UID is nil for PVC '%s'", pvc.Name)
		kms.Error(err)
		return err
	}

	if _, ok := kms.PersistentVolumeClaims[pvc.UID]; !ok {
		kms.PersistentVolumeClaims[pvc.UID] = pvc

		kms.Metadata.ObjectCount++
	}

	return nil
}
