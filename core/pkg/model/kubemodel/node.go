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
	UID           string            `json:"uid"`
	ProviderID    string            `json:"providerId"`
	Name          string            `json:"name"`
	Labels        map[string]string `json:"labels,omitempty"`
	Annotations   map[string]string `json:"annotations,omitempty"` // TODO unpopulated
	InstanceType  string            `json:"InstanceType"`
	Preemptible   bool              `json:"preemptible"` // TODO unpopulated
	CPUMilliCores Measurement       `json:"cpuMilliCores"`
	RAMBytes      Measurement       `json:"ramBytes"`
	GPUCount      Measurement       `json:"gpuCount"`
	//AttachedVolumes map[string]*NodeVolumeUsage `json:"attachedVolumes,omitempty"`
	Start time.Time `json:"start,omitempty"` // Node creation/start timestamp
	End   time.Time `json:"end,omitempty"`   // Node deletion/end timestamp (nil if still running)
}

//// NodeVolumeUsage tracks storage usage for a disk volume attached to a node.
//// Used for cost allocation of cloud storage resources (e.g., AWS EBS volumes).
//type NodeVolumeUsage struct {
//	VolumeUID        string      `json:"volumeUid"`        // "root" for primary disk, or actual volume UID for additional volumes
//	CapacityBytes    Measurement `json:"capacityBytes"`    // Total capacity of the volume in bytes
//	UsageByteSeconds Measurement `json:"usageByteSeconds"` // Cumulative usage (Byte × seconds) over measurement window
//	VolumeType       string      `json:"volumeType"`       // "root" for primary disk, "persistent" for additional PVs
//	ProviderID       string      `json:"providerId"`       // Cloud provider volume ID (e.g., "vol-xxxxx" for AWS EBS)
//	DurationSeconds  Measurement `json:"durationSeconds"`  // Duration the volume was attached during measurement window in seconds
//}

//// CpuMillicoreUsageAverage calculates the average CPU usage in millicores over the uptime period.
//// Returns 0 if uptime is 0 to avoid division by zero.
//func (n *Node) CpuMillicoreUsageAverage() Measurement {
//	if n.DurationSeconds == 0 {
//		return 0
//	}
//	return n.CpuMillicoreSeconds / n.DurationSeconds
//}
//
//// RAMByteUsageAverage calculates the average RAM usage in bytes over the uptime period.
//// Returns 0 if uptime is 0 to avoid division by zero.
//func (n *Node) RAMByteUsageAverage() Measurement {
//	if n.DurationSeconds == 0 {
//		return 0
//	}
//	return n.RAMByteSeconds / n.DurationSeconds
//}
//
//// TotalVolumeUsageByteSeconds returns the sum of all volume usage Byte-seconds across all attached volumes.
//func (n *Node) TotalVolumeUsageByteSeconds() Measurement {
//	var total Measurement
//	for _, volume := range n.AttachedVolumes {
//		total += volume.UsageByteSeconds
//	}
//	return total
//}
//
//// TotalVolumeCapacityBytes returns the sum of all volume capacities across all attached volumes.
//func (n *Node) TotalVolumeCapacityBytes() Measurement {
//	var total Measurement
//	for _, volume := range n.AttachedVolumes {
//		total += volume.CapacityBytes
//	}
//	return total
//}
//
//// GetVolumeUsageAverage calculates the average storage usage in bytes for a specific volume over the uptime period.
//// Returns 0 if uptime is 0 or volume doesn't exist.
//func (n *Node) GetVolumeUsageAverage(volumeUID string) Measurement {
//	volume, exists := n.AttachedVolumes[volumeUID]
//	if !exists || n.DurationSeconds == 0 {
//		return 0
//	}
//	return volume.UsageByteSeconds / n.DurationSeconds
//}

// RegisterNode validates and adds a node to the set
func (kms *KubeModelSet) RegisterNode(node *Node) error {
	// Check required fields
	if node.UID == "" {
		err := fmt.Errorf("UID is missing for Node with name '%s'", node.Name)
		kms.Error(err)
		return err
	}

	if node.Name == "" {
		err := fmt.Errorf("Name is missing for Node '%s'", node.UID)
		kms.Error(err)
		return err
	}

	if kms.Window.Start.After(node.Start) ||
		kms.Window.Start.After(node.End) ||
		kms.Window.End.Before(node.Start) ||
		kms.Window.End.Before(node.End) {
		err := fmt.Errorf(
			"Node '%s' has a start or end time (%s-%s) outside of the window %s-%s",
			node.Start.Format(time.RFC3339),
			node.End.Format(time.RFC3339),
			kms.Window.Start.Format(time.RFC3339),
			kms.Window.End.Format(time.RFC3339),
		)
		kms.Error(err)
		return err
	}

	if _, ok := kms.Nodes[node.UID]; !ok {
		if kms.Cluster == nil {
			kms.Warnf("RegisterNode: Cluster is nil")
		}

		kms.Nodes[node.UID] = node

		kms.Metadata.ObjectCount++
	}

	return nil
}
