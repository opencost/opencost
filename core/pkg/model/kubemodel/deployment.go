package kubemodel

import (
	"fmt"
	"time"
)

// @bingen:generate:Deployment
// Deployment represents a Kubernetes Deployment resource
type Deployment struct {
	UID          string            `json:"uid"`
	NamespaceUID string            `json:"namespaceUid"`
	Name         string            `json:"name"`
	Labels       map[string]string `json:"labels,omitempty"`
	Annotations  map[string]string `json:"annotations,omitempty"`
	MatchLabels  map[string]string `json:"matchLabels"`
	Start        time.Time         `json:"start"`
	End          time.Time         `json:"end"`
}

func (d *Deployment) ValidateDeployment(window Window) error {
	if d.UID == "" {
		return fmt.Errorf("UID is missing for Deployment with name '%s'", d.Name)
	}

	if d.Name == "" {
		return fmt.Errorf("Name is missing for Deployment '%s'", d.UID)
	}

	if d.NamespaceUID == "" {
		return fmt.Errorf("NamespaceUID is missing for Deployment '%s'", d.UID)
	}

	if err := checkWindow(window, d.Start, d.End); err != nil {
		return err
	}

	return nil
}

func (kms *KubeModelSet) RegisterDeployment(deployment *Deployment) error {
	if err := deployment.ValidateDeployment(kms.Window); err != nil {
		err = fmt.Errorf("RegisterDeployment: invalid deployment: %w", err)
		kms.Error(err)
		return err
	}

	if _, ok := kms.Deployments[deployment.UID]; !ok {
		if kms.Cluster == nil {
			kms.Warnf("RegisterDeployment: Cluster is nil")
		}

		kms.Deployments[deployment.UID] = deployment

		kms.Metadata.ObjectCount++
	}

	return nil
}
