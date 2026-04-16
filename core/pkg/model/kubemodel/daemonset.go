package kubemodel

import (
	"fmt"
	"time"
)

// @bingen:generate:DaemonSet
// DaemonSet represents a Kubernetes DaemonSet resource
type DaemonSet struct {
	UID          string            `json:"uid"`
	NamespaceUID string            `json:"namespaceUid"`
	Name         string            `json:"name"`
	Labels       map[string]string `json:"labels,omitempty"`
	Annotations  map[string]string `json:"annotations,omitempty"`
	Start        time.Time         `json:"start,omitempty"`
	End          time.Time         `json:"end,omitempty"`
}

func (kms *KubeModelSet) RegisterDaemonSet(daemonSet *DaemonSet) error {
	// Check required fields
	if daemonSet.UID == "" {
		err := fmt.Errorf("UID is missing for DaemonSet with name '%s'", daemonSet.Name)
		kms.Error(err)
		return err
	}

	if daemonSet.Name == "" {
		err := fmt.Errorf("Name is missing for DaemonSet '%s'", daemonSet.UID)
		kms.Error(err)
		return err
	}

	if daemonSet.NamespaceUID == "" {
		err := fmt.Errorf("NamespaceUID is missing for DaemonSet '%s'", daemonSet.UID)
		kms.Error(err)
		return err
	}

	if err := checkWindow(kms.Window, daemonSet.Start, daemonSet.End); err != nil {
		kms.Error(err)
		return err
	}

	if _, ok := kms.DaemonSets[daemonSet.UID]; !ok {
		if kms.Cluster == nil {
			kms.Warnf("RegisterDaemonSet: Cluster is nil")
		}

		kms.DaemonSets[daemonSet.UID] = daemonSet

		kms.Metadata.ObjectCount++
	}

	return nil
}