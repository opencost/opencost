package kubemodel

import (
	"fmt"
	"time"
)

// @bingen:generate:ReplicaSet
// ReplicaSet represents a Kubernetes ReplicaSet resource
type ReplicaSet struct {
	UID          string            `json:"uid"`
	NamespaceUID string            `json:"namespaceUid"`
	Name         string            `json:"name"`
	Owners       []Owner           `json:"owners"`
	Labels       map[string]string `json:"labels,omitempty"`
	Annotations  map[string]string `json:"annotations,omitempty"`
	Start        time.Time         `json:"start,omitempty"`
	End          time.Time         `json:"end,omitempty"`
}

func (kms *KubeModelSet) RegisterReplicaSet(replicaSet *ReplicaSet) error {
	// Check required fields
	if replicaSet.UID == "" {
		err := fmt.Errorf("UID is missing for ReplicaSet with name '%s'", replicaSet.Name)
		kms.Error(err)
		return err
	}

	if replicaSet.Name == "" {
		err := fmt.Errorf("Name is missing for ReplicaSet '%s'", replicaSet.UID)
		kms.Error(err)
		return err
	}

	if replicaSet.NamespaceUID == "" {
		err := fmt.Errorf("NamespaceUID is missing for ReplicaSet '%s'", replicaSet.UID)
		kms.Error(err)
		return err
	}

	if err := checkWindow(kms.Window, replicaSet.Start, replicaSet.End); err != nil {
		kms.Error(err)
		return err
	}

	if _, ok := kms.ReplicaSets[replicaSet.UID]; !ok {
		if kms.Cluster == nil {
			kms.Warnf("RegisterReplicaSet: Cluster is nil")
		}

		kms.ReplicaSets[replicaSet.UID] = replicaSet

		kms.Metadata.ObjectCount++
	}

	return nil
}
