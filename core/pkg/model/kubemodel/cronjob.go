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

func (c *CronJob) ValidateCronJob(window Window) error {
	if c.UID == "" {
		return fmt.Errorf("UID is missing for CronJob with name '%s'", c.Name)
	}

	if c.Name == "" {
		return fmt.Errorf("Name is missing for CronJob '%s'", c.UID)
	}

	if c.NamespaceUID == "" {
		return fmt.Errorf("NamespaceUID is missing for CronJob '%s'", c.UID)
	}

	if err := checkWindow(window, c.Start, c.End); err != nil {
		return err
	}

	return nil
}

func (kms *KubeModelSet) RegisterCronJob(cronJob *CronJob) error {
	if err := cronJob.ValidateCronJob(kms.Window); err != nil {
		kms.Errorf("RegisterCronJob: invalid cronjob: %w", err)
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
