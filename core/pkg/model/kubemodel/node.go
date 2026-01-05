package kubemodel

import (
	"fmt"
	"time"
)

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

func (kms *KubeModelSet) RegisterNode(uid, name string) error {
	if uid == "" {
		err := fmt.Errorf("UID is nil for Node '%s'", name)
		kms.Error(err)
		return err
	}

	if _, ok := kms.Nodes[uid]; !ok {
		clusterUID := ""

		if kms.Cluster == nil {
			kms.Warnf("RegisterNode(%s, %s): Cluster is nil", uid, name)
		} else {
			clusterUID = kms.Cluster.UID
		}

		kms.Nodes[uid] = &Node{
			UID:        uid,
			ClusterUID: clusterUID,
			Name:       name,
		}

		kms.Metadata.ObjectCount++
	}

	return nil
}
