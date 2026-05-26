package kubemodel

import (
	"fmt"
	"time"
)

// @bingen:generate:PersistentVolume
type PersistentVolume struct {
	// Version 1 fields
	UID          string            `json:"uid"`
	ClusterUID   string            `json:"clusterUid"`
	Name         string            `json:"name"`
	Namespace    string            `json:"namespace"`
	Labels       map[string]string `json:"labels,omitempty"`
	Annotations  map[string]string `json:"annotations,omitempty"`
	StorageClass string            `json:"storageClass"`
	SizeBytes    Measurement       `json:"size"`
	// awsElasticBlockStore, azureDisk, gcePersistentDisk, csi, nfs, local, etc.
	Type string `json:"type,omitempty"`
	// ebs.csi.aws.com, disk.csi.azure.com, etc.
	CSIDriver string `json:"csiDriver,omitempty"`
	// Cloud provider's volume identifier
	ProviderVolumeID string `json:"providerVolumeId,omitempty"`
	// ReadWriteOnce, ReadWriteMany, ReadOnlyMany
	AccessModes []string `json:"accessModes,omitempty"`
	// Retain, Delete, Recycle
	ReclaimPolicy string `json:"reclaimPolicy,omitempty"`
	// Cloud region for cross-region cost tracking
	Region string `json:"region,omitempty"`
	// Availability zone for cross-AZ cost tracking
	Zone string `json:"zone,omitempty"`
	// Volume lifecycle timestamps
	Start time.Time `json:"start"`         // Volume creation timestamp
	End   time.Time `json:"end,omitempty"` // Volume deletion timestamp (nil if still active)
	// Duration volume existed within measurement window
	DurationSeconds Measurement `json:"durationSeconds"`
	// JSON-encoded node affinity for local volumes
	NodeAffinity string `json:"nodeAffinity,omitempty"`
	// Storage performance characteristics
	ProvisionedIOPS       Measurement `json:"provisionedIops,omitempty"`       // Provisioned IOPS (AWS io1/io2, Azure Premium)
	ProvisionedThroughput Measurement `json:"provisionedThroughput,omitempty"` // Provisioned throughput in MB/s
	PerformanceMode       string      `json:"performanceMode,omitempty"`       // "generalPurpose", "maxIO", "provisioned"
}

func (kms *KubeModelSet) RegisterVolume(uid, name string) error {
	if uid == "" {
		err := fmt.Errorf("UID is nil for PersistentVolume '%s'", name)
		kms.Error(err)
		return err
	}

	if _, ok := kms.Volumes[uid]; !ok {
		clusterUID := ""

		if kms.Cluster == nil {
			kms.Warnf("RegisterVolume(%s, %s): Cluster is nil", uid, name)
		} else {
			clusterUID = kms.Cluster.UID
		}

		kms.Volumes[uid] = &PersistentVolume{
			UID:        uid,
			ClusterUID: clusterUID,
			Name:       name,
		}

		kms.Metadata.ObjectCount++
	}

	return nil
}
