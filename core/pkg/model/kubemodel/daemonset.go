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

func (d *DaemonSet) ValidateDaemonSet(window Window) error {
	if d.UID == "" {
		return fmt.Errorf("UID is missing for DaemonSet with name '%s'", d.Name)
	}

	if d.Name == "" {
		return fmt.Errorf("Name is missing for DaemonSet '%s'", d.UID)
	}

	if d.NamespaceUID == "" {
		return fmt.Errorf("NamespaceUID is missing for DaemonSet '%s'", d.UID)
	}

	if err := checkWindow(window, d.Start, d.End); err != nil {
		return err
	}

	return nil
}

func (kms *KubeModelSet) RegisterDaemonSet(daemonSet *DaemonSet) error {
	if err := daemonSet.ValidateDaemonSet(kms.Window); err != nil {
		err = fmt.Errorf("RegisterDaemonSet: invalid daemonset: %w", err)
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
