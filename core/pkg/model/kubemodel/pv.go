package kubemodel

import (
	"fmt"
	"time"
)

// @bingen:generate:PersistentVolume
type PersistentVolume struct {
	UID             string      `json:"uid"`
	Name            string      `json:"name"`
	StorageClass    string      `json:"storageClass"`
	SizeBytes       Measurement `json:"size"`
	CSIVolumeHandle string      `json:"csiVolumeHandle,omitempty"`
	Start           time.Time   `json:"start"`
	End             time.Time   `json:"end"`
}

func (kms *KubeModelSet) RegisterPersistentVolume(pv *PersistentVolume) error {
	if pv.UID == "" {
		err := fmt.Errorf("UID is nil for PersistentVolume '%s'", pv.Name)
		kms.Error(err)
		return err
	}

	if _, ok := kms.PersistentVolumes[pv.UID]; !ok {
		kms.PersistentVolumes[pv.UID] = pv

		kms.Metadata.ObjectCount++
	}

	return nil
}
