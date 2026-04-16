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

	if cronJob.NamespaceUID == "" {
		err := fmt.Errorf("NamespaceUID is missing for CronJob '%s'", cronJob.UID)
		kms.Error(err)
		return err
	}

	if err := checkWindow(kms.Window, cronJob.Start, cronJob.End); err != nil {
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