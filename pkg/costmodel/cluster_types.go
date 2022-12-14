package costmodel

import "time"

type Node struct {
	Cluster         string
	Name            string
	ProviderID      string
	NodeType        string
	CPUCost         float64
	CPUCores        float64
	GPUCost         float64
	GPUCount        float64
	RAMCost         float64
	RAMBytes        float64
	Discount        float64
	Preemptible     bool
	CPUBreakdown    *ClusterCostsBreakdown
	RAMBreakdown    *ClusterCostsBreakdown
	Start           time.Time
	End             time.Time
	Minutes         float64
	Labels          map[string]string
	CostPerCPUHr    float64
	CostPerRAMGiBHr float64
	CostPerGPUHr    float64
}

type NodeIdentifier struct {
	Cluster    string
	Name       string
	ProviderID string
}

type nodeIdentifierNoProviderID struct {
	Cluster string
	Name    string
}

type Disk struct {
	Cluster        string
	Name           string
	ProviderID     string
	StorageClass   string
	VolumeName     string
	ClaimName      string
	ClaimNamespace string
	Cost           float64
	Bytes          float64

	// These two fields may not be available at all times because they rely on
	// a new set of metrics that may or may not be available. Thus, they must
	// be nilable to represent the complete absence of the data.
	//
	// In other words, nilability here lets us distinguish between
	// "metric is not available" and "metric is available but is 0".
	//
	// They end in "Ptr" to distinguish from an earlier version in order to
	// ensure that all usages are checked for nil.
	BytesUsedAvgPtr *float64
	BytesUsedMaxPtr *float64

	Local     bool
	Start     time.Time
	End       time.Time
	Minutes   float64
	Breakdown *ClusterCostsBreakdown
}

type DiskIdentifier struct {
	Cluster string
	Name    string
}

type LoadBalancerIdentifier struct {
	Cluster   string
	Namespace string
	Name      string
}

type LoadBalancer struct {
	Cluster    string
	Namespace  string
	Name       string
	ProviderID string
	Cost       float64
	Start      time.Time
	End        time.Time
	Minutes    float64
}

type ClusterManagementIdentifier struct {
	Cluster     string
	Provisioner string
}

type ClusterManagement struct {
	Cluster     string
	Provisioner string
	Cost        float64
	Start       time.Time
	End         time.Time
	Minutes     float64
}
