package kubemodel

import (
	"fmt"
	"time"
)

// @bingen:generate:Job
// Job represents a Kubernetes Job resource
type Job struct {
	UID          string            `json:"uid"`
	NamespaceUID string            `json:"namespaceUid"`
	Name         string            `json:"name"`
	Labels       map[string]string `json:"labels,omitempty"`
	Annotations  map[string]string `json:"annotations,omitempty"`
	Start        time.Time         `json:"start,omitempty"`
	End          time.Time         `json:"end,omitempty"`
}

func (kms *KubeModelSet) RegisterJob(job *Job) error {
	// Check required fields
	if job.UID == "" {
		err := fmt.Errorf("UID is missing for Job with name '%s'", job.Name)
		kms.Error(err)
		return err
	}

	if job.Name == "" {
		err := fmt.Errorf("Name is missing for Job '%s'", job.UID)
		kms.Error(err)
		return err
	}

	if kms.Window.Start.After(job.Start) ||
		kms.Window.Start.After(job.End) ||
		kms.Window.End.Before(job.Start) ||
		kms.Window.End.Before(job.End) {
		err := fmt.Errorf(
			"Job '%s' has a start or end time (%s-%s) outside of the window %s-%s",
			job.Name,
			job.Start.Format(time.RFC3339),
			job.End.Format(time.RFC3339),
			kms.Window.Start.Format(time.RFC3339),
			kms.Window.End.Format(time.RFC3339),
		)
		kms.Error(err)
		return err
	}

	if _, ok := kms.Jobs[job.UID]; !ok {
		if kms.Cluster == nil {
			kms.Warnf("RegisterJob: Cluster is nil")
		}

		kms.Jobs[job.UID] = job

		kms.Metadata.ObjectCount++
	}

	return nil
}