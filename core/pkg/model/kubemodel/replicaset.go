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

func (r *ReplicaSet) ValidateReplicaSet(window Window) error {
	if r.UID == "" {
		return fmt.Errorf("UID is missing for ReplicaSet with name '%s'", r.Name)
	}

	if r.Name == "" {
		return fmt.Errorf("Name is missing for ReplicaSet '%s'", r.UID)
	}

	if r.NamespaceUID == "" {
		return fmt.Errorf("NamespaceUID is missing for ReplicaSet '%s'", r.UID)
	}

	if err := checkWindow(window, r.Start, r.End); err != nil {
		return err
	}

	return nil
}

func (kms *KubeModelSet) RegisterReplicaSet(replicaSet *ReplicaSet) error {
	if err := replicaSet.ValidateReplicaSet(kms.Window); err != nil {
		kms.Errorf("RegisterReplicaSet: invalid replicaset: %w", err)
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
