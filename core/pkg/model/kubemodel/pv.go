package kubemodel

import (
	"fmt"
	"time"
)

// @bingen:generate:PersistentVolume
type PersistentVolume struct {
	UID             string    `json:"uid"`
	Name            string    `json:"name"`
	StorageClass    string    `json:"storageClass"`
	CSIVolumeHandle string    `json:"csiVolumeHandle,omitempty"`
	SizeBytes       float64   `json:"size"`
	Start           time.Time `json:"start"`
	End             time.Time `json:"end"`
}

func (kms *KubeModelSet) RegisterPersistentVolume(pv *PersistentVolume) error {
	if pv.UID == "" {
		err := fmt.Errorf("UID is missing for PersistentVolume with name '%s'", pv.Name)
		kms.Error(err)
		return err
	}

	if pv.Name == "" {
		err := fmt.Errorf("Name is missing for PersistentVolume '%s'", pv.UID)
		kms.Error(err)
		return err
	}

	if err := checkWindow(kms.Window, pv.Start, pv.End); err != nil {
		kms.Error(err)
		return err
	}

	if _, ok := kms.PersistentVolumes[pv.UID]; !ok {
		kms.PersistentVolumes[pv.UID] = pv

		kms.Metadata.ObjectCount++
	}

	return nil
}
