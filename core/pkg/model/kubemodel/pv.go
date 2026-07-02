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

func (p *PersistentVolume) ValidatePersistentVolume(window Window) error {
	if p.UID == "" {
		return fmt.Errorf("UID is missing for PersistentVolume with name '%s'", p.Name)
	}

	if p.Name == "" {
		return fmt.Errorf("Name is missing for PersistentVolume '%s'", p.UID)
	}

	if err := checkWindow(window, p.Start, p.End); err != nil {
		return err
	}

	return nil
}

func (kms *KubeModelSet) RegisterPersistentVolume(pv *PersistentVolume) error {
	if err := pv.ValidatePersistentVolume(kms.Window); err != nil {
		err = fmt.Errorf("RegisterPersistentVolume: invalid persistent volume: %w", err)
		kms.Error(err)
		return err
	}

	if _, ok := kms.PersistentVolumes[pv.UID]; !ok {
		kms.PersistentVolumes[pv.UID] = pv

		kms.Metadata.ObjectCount++
	}

	return nil
}
