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
	PVCVolumes            []PodPVCVolumes        `json:"pvcVolumes"`
	Labels                map[string]string      `json:"labels,omitempty"`
	Annotations           map[string]string      `json:"annotations,omitempty"`
	NetworkTrafficDetails []NetworkTrafficDetail `json:"networkTrafficDetails,omitempty"`
	Start                 time.Time              `json:"start"`
	End                   time.Time              `json:"end"`
}

func (kms *KubeModelSet) RegisterPod(pod *Pod) error {
	// Check required fields
	if pod.UID == "" {
		err := fmt.Errorf("UID is missing for Pod with name '%s'", pod.Name)
		kms.Error(err)
		return err
	}

	if pod.Name == "" {
		err := fmt.Errorf("Name is missing for Pod '%s'", pod.UID)
		kms.Error(err)
		return err
	}

	if pod.NamespaceUID == "" {
		err := fmt.Errorf("NamespaceUID is missing for Pod '%s'", pod.UID)
		kms.Error(err)
		return err
	}

	if err := checkWindow(kms.Window, pod.Start, pod.End); err != nil {
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
