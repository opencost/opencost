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

func (j *Job) ValidateJob(window Window) error {
	if j.UID == "" {
		return fmt.Errorf("UID is missing for Job with name '%s'", j.Name)
	}

	if j.Name == "" {
		return fmt.Errorf("Name is missing for Job '%s'", j.UID)
	}

	if j.NamespaceUID == "" {
		return fmt.Errorf("NamespaceUID is missing for Job '%s'", j.UID)
	}

	if err := checkWindow(window, j.Start, j.End); err != nil {
		return err
	}

	return nil
}

func (kms *KubeModelSet) RegisterJob(job *Job) error {
	if err := job.ValidateJob(kms.Window); err != nil {
		err = fmt.Errorf("RegisterJob: invalid job: %w", err)
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
