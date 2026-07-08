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

func (p *PersistentVolumeClaim) ValidatePVC(window Window) error {
	if p.UID == "" {
		return fmt.Errorf("UID is missing for PVC with name '%s'", p.Name)
	}

	if p.Name == "" {
		return fmt.Errorf("Name is missing for PVC '%s'", p.UID)
	}

	if p.NamespaceUID == "" {
		return fmt.Errorf("NamespaceUID is missing for PVC '%s'", p.UID)
	}

	if err := checkWindow(window, p.Start, p.End); err != nil {
		return err
	}

	return nil
}

func (kms *KubeModelSet) RegisterPVC(pvc *PersistentVolumeClaim) error {
	if err := pvc.ValidatePVC(kms.Window); err != nil {
		err = fmt.Errorf("RegisterPVC: invalid pvc: %w", err)
		kms.Error(err)
		return err
	}

	if _, ok := kms.PersistentVolumeClaims[pvc.UID]; !ok {
		kms.PersistentVolumeClaims[pvc.UID] = pvc

		kms.Metadata.ObjectCount++
	}

	return nil
}
