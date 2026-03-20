package kubemodel

import (
	"fmt"
	"time"
)

// @bingen:generate:CronJob
// CronJob represents a Kubernetes CronJob resource
type CronJob struct {
	UID          string            `json:"uid"`
	NamespaceUID string            `json:"namespaceUid"`
	Name         string            `json:"name"`
	Labels       map[string]string `json:"labels,omitempty"`
	Annotations  map[string]string `json:"annotations,omitempty"`
	Start        time.Time         `json:"start,omitempty"`
	End          time.Time         `json:"end,omitempty"`
}

func (kms *KubeModelSet) RegisterCronJob(cronJob *CronJob) error {
	// Check required fields
	if cronJob.UID == "" {
		err := fmt.Errorf("UID is missing for CronJob with name '%s'", cronJob.Name)
		kms.Error(err)
		return err
	}

	if cronJob.Name == "" {
		err := fmt.Errorf("Name is missing for CronJob '%s'", cronJob.UID)
		kms.Error(err)
		return err
	}

	if kms.Window.Start.After(cronJob.Start) ||
		kms.Window.Start.After(cronJob.End) ||
		kms.Window.End.Before(cronJob.Start) ||
		kms.Window.End.Before(cronJob.End) {
		err := fmt.Errorf(
			"CronJob '%s' has a start or end time (%s-%s) outside of the window %s-%s",
			cronJob.Name,
			cronJob.Start.Format(time.RFC3339),
			cronJob.End.Format(time.RFC3339),
			kms.Window.Start.Format(time.RFC3339),
			kms.Window.End.Format(time.RFC3339),
		)
		kms.Error(err)
		return err
	}

	if _, ok := kms.CronJobs[cronJob.UID]; !ok {
		if kms.Cluster == nil {
			kms.Warnf("RegisterCronJob: Cluster is nil")
		}

		kms.CronJobs[cronJob.UID] = cronJob

		kms.Metadata.ObjectCount++
	}

	return nil
}