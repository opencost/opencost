package kubemodel

import (
	"fmt"
	"time"
)

// @bingen:generate:PodPVCVolumes
type PodPVCVolumes struct {
	Name                     string `json:"name"`
	PersistentVolumeClaimUID string `json:"persistentVolumeClaimUID"`
}

// @bingen:generate:Pod
type Pod struct {
	UID                   string                 `json:"uid"`
	NamespaceUID          string                 `json:"namespaceUid"`
	NodeUID               string                 `json:"nodeUid"`
	Name                  string                 `json:"name"`
	Owners                []Owner                `json:"owners"`
	PVCVolumes            []PodPVCVolumes        `json:"pvcVolumes,omitempty"`
	Labels                map[string]string      `json:"labels,omitempty"`
	Annotations           map[string]string      `json:"annotations,omitempty"`
	NetworkTrafficDetails []NetworkTrafficDetail `json:"networkTrafficDetails,omitempty"`
	Start                 time.Time              `json:"start"`
	End                   time.Time              `json:"end"`
}

func (p *Pod) ValidatePod(window Window) error {
	if p.UID == "" {
		return fmt.Errorf("UID is missing for Pod with name '%s'", p.Name)
	}

	if p.Name == "" {
		return fmt.Errorf("Name is missing for Pod '%s'", p.UID)
	}

	if p.NamespaceUID == "" {
		return fmt.Errorf("NamespaceUID is missing for Pod '%s'", p.UID)
	}

	if err := checkWindow(window, p.Start, p.End); err != nil {
		return err
	}

	return nil
}

func (kms *KubeModelSet) RegisterPod(pod *Pod) error {
	if err := pod.ValidatePod(kms.Window); err != nil {
		err = fmt.Errorf("RegisterPod: invalid pod: %w", err)
		kms.Error(err)
		return err
	}

	if _, ok := kms.Pods[pod.UID]; !ok {
		if kms.Cluster == nil {
			kms.Warnf("RegisterPod: Cluster is nil")
		}

		kms.Pods[pod.UID] = pod

		kms.Metadata.ObjectCount++
	}

	return nil
}
