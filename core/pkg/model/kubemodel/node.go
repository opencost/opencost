package kubemodel

import "time"

type Node struct {
	UID                          string            `json:"uid"`
	ClusterUID                   string            `json:"clusterUid"`
	ProviderResourceUID          string            `json:"providerResourceUid"`
	Name                         string            `json:"name"`
	Labels                       map[string]string `json:"labels,omitempty"`
	Annotations                  map[string]string `json:"annotations,omitempty"`
	Start                        time.Time         `json:"start"`
	End                          time.Time         `json:"end"`
	CpuMillicoreSecondsAllocated uint64            `json:"cpuMillicoreSecondsAllocated"`
	RAMByteSecondsAllocated      uint64            `json:"ramByteSecondsAllocated"`
	// PublicIPSeconds represents the cumulative public IP allocation (count × seconds) for this node.
	// Calculated as: number of ExternalIP addresses from Kubernetes node Status.Addresses × window duration in seconds.
	// Used for cost attribution of public IP addresses associated with the node.
	PublicIPSecondsAllocated uint64 `json:"publicIpSecondsAllocated"`
	CpuMillicoreUsageAverage uint64 `json:"cpuMillicoreUsageAverage"`
	CpuMillicoreUsageMax     uint64 `json:"cpuMillicoreUsageMax"`
	RAMByteUsageAverage      uint64 `json:"ramByteUsageAverage"`
	RAMByteUsageMax          uint64 `json:"ramByteUsageMax"`
}
