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

	if job.NamespaceUID == "" {
		err := fmt.Errorf("NamespaceUID is missing for Job '%s'", job.UID)
		kms.Error(err)
		return err
	}

	if err := checkWindow(kms.Window, job.Start, job.End); err != nil {
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