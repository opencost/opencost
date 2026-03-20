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

func (kms *KubeModelSet) RegisterDeployment(deployment *Deployment) error {
	// Check required fields
	if deployment.UID == "" {
		err := fmt.Errorf("UID is missing for Deployment with name '%s'", deployment.Name)
		kms.Error(err)
		return err
	}

	if deployment.Name == "" {
		err := fmt.Errorf("Name is missing for Deployment '%s'", deployment.UID)
		kms.Error(err)
		return err
	}

	if kms.Window.Start.After(deployment.Start) ||
		kms.Window.Start.After(deployment.End) ||
		kms.Window.End.Before(deployment.Start) ||
		kms.Window.End.Before(deployment.End) {
		err := fmt.Errorf(
			"Deployment '%s' has a start or end time (%s-%s) outside of the window %s-%s",
			deployment.Name,
			deployment.Start.Format(time.RFC3339),
			deployment.End.Format(time.RFC3339),
			kms.Window.Start.Format(time.RFC3339),
			kms.Window.End.Format(time.RFC3339),
		)
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
