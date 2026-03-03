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

	if kms.Window.Start.After(statefulSet.Start) ||
		kms.Window.Start.After(statefulSet.End) ||
		kms.Window.End.Before(statefulSet.Start) ||
		kms.Window.End.Before(statefulSet.End) {
		err := fmt.Errorf(
			"StatefulSet '%s' has a start or end time (%s-%s) outside of the window %s-%s",
			statefulSet.Name,
			statefulSet.Start.Format(time.RFC3339),
			statefulSet.End.Format(time.RFC3339),
			kms.Window.Start.Format(time.RFC3339),
			kms.Window.End.Format(time.RFC3339),
		)
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