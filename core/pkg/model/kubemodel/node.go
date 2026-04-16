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
	UID                  string             `json:"uid"`
	ProviderID           string             `json:"providerId"`
	Name                 string             `json:"name"`
	Labels               map[string]string  `json:"labels"`
	InstanceType         string             `json:"instanceType"`
	Preemptible          bool               `json:"preemptible"` // TODO unpopulated
	ResourceCapacities   ResourceQuantities `json:"resourceCapacities"`
	ResourcesAllocatable ResourceQuantities `json:"resourcesAllocatable"`
	FileSystem           FileSystem         `json:"fileSystem"`
	Start                time.Time          `json:"start"`
	End                  time.Time          `json:"end"`
}

// @bingen:generate:FileSystem
// FileSystem records information for a nodes local storage
type FileSystem struct {
	CapacityBytes float64 `json:"capacityBytes"` // Total capacity of the volume in bytes
	UsageByteAvg  float64 `json:"usageByteAvg"`
	UsageByteMax  float64 `json:"usageByteMax"`
}

func (n *Node) ValidateNode(window Window) error {
	if n.UID == "" {
		return fmt.Errorf("UID is missing for Node with name '%s'", n.Name)
	}

	if n.Name == "" {
		return fmt.Errorf("Name is missing for Node '%s'", n.UID)
	}

	if err := checkWindow(window, n.Start, n.End); err != nil {
		return err
	}

	return nil
}

// RegisterNode validates and adds a node to the set
func (kms *KubeModelSet) RegisterNode(node *Node) error {
	if err := node.ValidateNode(kms.Window); err != nil {
		kms.Errorf("RegisterNode: invalid node: %w", err)
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
