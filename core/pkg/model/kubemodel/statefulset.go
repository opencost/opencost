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

func (s *StatefulSet) ValidateStatefulSet(window Window) error {
	if s.UID == "" {
		return fmt.Errorf("UID is missing for StatefulSet with name '%s'", s.Name)
	}

	if s.Name == "" {
		return fmt.Errorf("Name is missing for StatefulSet '%s'", s.UID)
	}

	if s.NamespaceUID == "" {
		return fmt.Errorf("NamespaceUID is missing for StatefulSet '%s'", s.UID)
	}

	if err := checkWindow(window, s.Start, s.End); err != nil {
		return err
	}

	return nil
}

func (kms *KubeModelSet) RegisterStatefulSet(statefulSet *StatefulSet) error {
	if err := statefulSet.ValidateStatefulSet(kms.Window); err != nil {
		kms.Errorf("RegisterStatefulSet: invalid statefulset: %w", err)
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
