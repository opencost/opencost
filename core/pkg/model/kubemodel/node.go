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
	InstanceType  string            `json:"instanceType"`
	Preemptible   bool              `json:"preemptible"` // TODO unpopulated
	CPUMilliCores float64           `json:"cpuMilliCores"`
	RAMBytes      float64           `json:"ramBytes"`
	GPUCount      float64           `json:"gpuCount"`
	FileSystem    FileSystem        `json:"fileSystem"`
	Start         time.Time         `json:"start,omitempty"` // Node creation/start timestamp
	End           time.Time         `json:"end,omitempty"`   // Node deletion/end timestamp (nil if still running)
}

// @bingen:generate:FileSystem
// NodeVolumeUsage tracks storage usage for a disk volume attached to a node.
// Used for cost allocation of cloud storage resources (e.g., AWS EBS volumes).
type FileSystem struct {
	CapacityBytes float64 `json:"capacityBytes"` // Total capacity of the volume in bytes
	UsageByteAvg  float64 `json:"usageByteAvg"`
	UsageByteMax  float64 `json:"usageByteMax"`
}

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
