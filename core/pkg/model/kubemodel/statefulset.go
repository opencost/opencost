package kubemodel

import (
	"fmt"
	"time"
)

// @bingen:generate:StatefulSet
// StatefulSet represents a Kubernetes StatefulSet resource
type StatefulSet struct {
	UID          string            `json:"uid"`
	NamespaceUID string            `json:"namespaceUid"`
	Name         string            `json:"name"`
	Labels       map[string]string `json:"labels,omitempty"`
	Annotations  map[string]string `json:"annotations,omitempty"`
	MatchLabels  map[string]string `json:"matchLabels,omitempty"`
	Start        time.Time         `json:"start,omitempty"`
	End          time.Time         `json:"end,omitempty"`
}

func (kms *KubeModelSet) RegisterStatefulSet(statefulSet *StatefulSet) error {
	// Check required fields
	if statefulSet.UID == "" {
		err := fmt.Errorf("UID is missing for StatefulSet with name '%s'", statefulSet.Name)
		kms.Error(err)
		return err
	}

	if statefulSet.Name == "" {
		err := fmt.Errorf("Name is missing for StatefulSet '%s'", statefulSet.UID)
		kms.Error(err)
		return err
	}

	if statefulSet.NamespaceUID == "" {
		err := fmt.Errorf("NamespaceUID is missing for StatefulSet '%s'", statefulSet.UID)
		kms.Error(err)
		return err
	}

	if err := checkWindow(kms.Window, statefulSet.Start, statefulSet.End); err != nil {
		kms.Error(err)
		return err
	}

	if _, ok := kms.StatefulSets[statefulSet.UID]; !ok {
		if kms.Cluster == nil {
			kms.Warnf("RegisterStatefulSet: Cluster is nil")
		}

		kms.StatefulSets[statefulSet.UID] = statefulSet

		kms.Metadata.ObjectCount++
	}

	return nil
}