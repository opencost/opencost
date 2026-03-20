package kubemodel

import (
	"fmt"
	"time"
)

// @bingen:generate:Node
// Node represents a Kubernetes node with capacity-based resource tracking.
// All resource measures (CPU, RAM) represent node capacity, not requests or limits.
// This aligns with the principle that cost allocation should be based on provisioned capacity.
type Node struct {
	UID                  string                      `json:"uid"`
	ProviderResourceUID  string                      `json:"providerResourceUid"`
	Name                 string                      `json:"name"`
	Labels               map[string]string           `json:"labels,omitempty"`
	Annotations          map[string]string           `json:"annotations,omitempty"`
	DurationSeconds      Measurement                 `json:"durationSeconds"`
	CpuMillicoreSeconds  Measurement                 `json:"cpuMillicoreSeconds"` // Node CPU capacity in millicore-seconds
	RAMByteSeconds       Measurement                 `json:"ramByteSeconds"`      // Node RAM capacity in Byte-seconds
	AttachedVolumes      map[string]*NodeVolumeUsage `json:"attachedVolumes,omitempty"`
	CpuMillicoreUsageMax Measurement                 `json:"cpuMillicoreUsageMax"` // Peak CPU usage observed
	RAMByteUsageMax      Measurement                 `json:"ramByteUsageMax"`      // Peak RAM usage observed
	Start                time.Time                   `json:"start,omitempty"`      // Node creation/start timestamp
	End                  time.Time                   `json:"end,omitempty"`        // Node deletion/end timestamp (nil if still running)
}

// NodeVolumeUsage tracks storage usage for a disk volume attached to a node.
// Used for cost allocation of cloud storage resources (e.g., AWS EBS volumes).
type NodeVolumeUsage struct {
	VolumeUID        string      `json:"volumeUid"`        // "root" for primary disk, or actual volume UID for additional volumes
	CapacityBytes    Measurement `json:"capacityBytes"`    // Total capacity of the volume in bytes
	UsageByteSeconds Measurement `json:"usageByteSeconds"` // Cumulative usage (Byte × seconds) over measurement window
	VolumeType       string      `json:"volumeType"`       // "root" for primary disk, "persistent" for additional PVs
	ProviderID       string      `json:"providerId"`       // Cloud provider volume ID (e.g., "vol-xxxxx" for AWS EBS)
	DurationSeconds  Measurement `json:"durationSeconds"`  // Duration the volume was attached during measurement window in seconds
}

// CpuMillicoreUsageAverage calculates the average CPU usage in millicores over the uptime period.
// Returns 0 if uptime is 0 to avoid division by zero.
func (n *Node) CpuMillicoreUsageAverage() Measurement {
	if n.DurationSeconds == 0 {
		return 0
	}
	return n.CpuMillicoreSeconds / n.DurationSeconds
}

// RAMByteUsageAverage calculates the average RAM usage in bytes over the uptime period.
// Returns 0 if uptime is 0 to avoid division by zero.
func (n *Node) RAMByteUsageAverage() Measurement {
	if n.DurationSeconds == 0 {
		return 0
	}
	return n.RAMByteSeconds / n.DurationSeconds
}

// TotalVolumeUsageByteSeconds returns the sum of all volume usage Byte-seconds across all attached volumes.
func (n *Node) TotalVolumeUsageByteSeconds() Measurement {
	var total Measurement
	for _, volume := range n.AttachedVolumes {
		total += volume.UsageByteSeconds
	}
	return total
}

// TotalVolumeCapacityBytes returns the sum of all volume capacities across all attached volumes.
func (n *Node) TotalVolumeCapacityBytes() Measurement {
	var total Measurement
	for _, volume := range n.AttachedVolumes {
		total += volume.CapacityBytes
	}
	return total
}

// GetVolumeUsageAverage calculates the average storage usage in bytes for a specific volume over the uptime period.
// Returns 0 if uptime is 0 or volume doesn't exist.
func (n *Node) GetVolumeUsageAverage(volumeUID string) Measurement {
	volume, exists := n.AttachedVolumes[volumeUID]
	if !exists || n.DurationSeconds == 0 {
		return 0
	}
	return volume.UsageByteSeconds / n.DurationSeconds
}

func (kms *KubeModelSet) RegisterNode(uid, name string) error {
	if uid == "" {
		err := fmt.Errorf("UID is nil for Node '%s'", name)
		kms.Error(err)
		return err
	}

	if _, ok := kms.Nodes[uid]; !ok {
		if kms.Cluster == nil {
			kms.Warnf("RegisterNode(%s, %s): Cluster is nil", uid, name)
		}

		kms.Nodes[uid] = &Node{
			UID:             uid,
			Name:            name,
			AttachedVolumes: make(map[string]*NodeVolumeUsage),
		}

		kms.Metadata.ObjectCount++
	}

	return nil
}
