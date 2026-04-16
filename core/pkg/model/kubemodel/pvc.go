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
		err := fmt.Errorf("UID is missing for PVC with name '%s'", pvc.Name)
		kms.Error(err)
		return err
	}

	if pvc.Name == "" {
		err := fmt.Errorf("Name is missing for PVC '%s'", pvc.UID)
		kms.Error(err)
		return err
	}

	if pvc.NamespaceUID == "" {
		err := fmt.Errorf("NamespaceUID is missing for PVC '%s'", pvc.UID)
		kms.Error(err)
		return err
	}

	if err := checkWindow(kms.Window, pvc.Start, pvc.End); err != nil {
		kms.Error(err)
		return err
	}

	if _, ok := kms.PersistentVolumeClaims[pvc.UID]; !ok {
		kms.PersistentVolumeClaims[pvc.UID] = pvc

		kms.Metadata.ObjectCount++
	}

	return nil
}
