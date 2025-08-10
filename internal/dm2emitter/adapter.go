//go:build dm2emitter

package dm2emitter

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/clusters"
)

// kubeInv adapts the existing OpenCost caches to our Inventory interface
type kubeInv struct {
	cache       clustercache.ClusterCache
	clusterInfo clusters.ClusterInfoProvider
}

// NewKubeInventory creates an adapter to the real OpenCost caches
func NewKubeInventory(cache clustercache.ClusterCache, clusterInfo clusters.ClusterInfoProvider) Inventory {
	return &kubeInv{
		cache:       cache,
		clusterInfo: clusterInfo,
	}
}

func (k *kubeInv) ListNamespaces(ctx context.Context) []Namespace {
	nsList := k.cache.GetAllNamespaces()
	result := make([]Namespace, 0, len(nsList))
	for _, ns := range nsList {
		result = append(result, Namespace{
			UID:  string(ns.UID),
			Name: ns.Name,
		})
	}
	return result
}

func (k *kubeInv) ListWorkloadsByNamespace(ctx context.Context, nsUID string) []Workload {
	// Get all pods and derive workloads from their owner references
	pods := k.cache.GetAllPods()
	workloadMap := make(map[string]Workload)

	// Find the namespace name for this UID
	var namespaceName string
	for _, ns := range k.cache.GetAllNamespaces() {
		if string(ns.UID) == nsUID {
			namespaceName = ns.Name
			break
		}
	}

	for _, pod := range pods {
		if pod.Namespace != namespaceName {
			continue
		}

		// Find the top-level controller (workload)
		if len(pod.OwnerReferences) > 0 {
			for _, owner := range pod.OwnerReferences {
				if owner.Controller != nil && *owner.Controller {
					wlUID := string(owner.UID)
					if _, exists := workloadMap[wlUID]; !exists {
						workloadMap[wlUID] = Workload{
							UID:          wlUID,
							Name:         owner.Name,
							Kind:         owner.Kind,
							NamespaceUID: nsUID,
						}
					}
					break
				}
			}
		}
	}

	result := make([]Workload, 0, len(workloadMap))
	for _, wl := range workloadMap {
		result = append(result, wl)
	}
	return result
}

func (k *kubeInv) ListPodsByWorkload(ctx context.Context, wlUID string) []Pod {
	pods := k.cache.GetAllPods()
	result := make([]Pod, 0)

	for _, pod := range pods {
		// Check if this pod belongs to the workload
		for _, owner := range pod.OwnerReferences {
			if owner.Controller != nil && *owner.Controller && string(owner.UID) == wlUID {
				nodeUID := ""
				if pod.Spec.NodeName != "" {
					// Try to get node UID
					nodes := k.cache.GetAllNodes()
					for _, node := range nodes {
						if node.Name == pod.Spec.NodeName {
							nodeUID = string(node.UID)
							break
						}
					}
				}

				result = append(result, Pod{
					UID:         string(pod.UID),
					Name:        pod.Name,
					NodeUID:     nodeUID,
					WorkloadUID: wlUID,
				})
				break
			}
		}
	}
	return result
}

func (k *kubeInv) ListContainersByPod(ctx context.Context, podUID string) []Container {
	pods := k.cache.GetAllPods()
	for _, pod := range pods {
		if string(pod.UID) != podUID {
			continue
		}

		result := make([]Container, 0, len(pod.Spec.Containers))
		for i, container := range pod.Spec.Containers {
			// Derive a stable container UID as podUID/containerName
			containerUID := fmt.Sprintf("%s/%s", podUID, container.Name)
			
			// Get the image from ContainerStatus if available
			image := ""
			if i < len(pod.Status.ContainerStatuses) && 
			   pod.Status.ContainerStatuses[i].Name == container.Name {
				image = pod.Status.ContainerStatuses[i].Image
			} else {
				// If not found by index, search by name
				for _, cs := range pod.Status.ContainerStatuses {
					if cs.Name == container.Name {
						image = cs.Image
						break
					}
				}
			}
			
			result = append(result, Container{
				UID:    containerUID,
				Name:   container.Name,
				Image:  image,
				PodUID: podUID,
			})
		}
		return result
	}
	return nil
}

func (k *kubeInv) ClusterUID(ctx context.Context) string {
	// First check environment variable
	if uid := os.Getenv("OPENCOST_CLUSTER_UID"); uid != "" {
		return uid
	}

	// Try to get from cluster info
	if k.clusterInfo != nil {
		info := k.clusterInfo.GetClusterInfo()
		if id, ok := info["id"]; ok && id != "" {
			return id
		}
		if name, ok := info["name"]; ok && name != "" {
			// Generate stable hash from cluster name
			h := sha256.Sum256([]byte(name))
			return hex.EncodeToString(h[:8]) // Use first 8 bytes for shorter UID
		}
	}

	return "unknown"
}

func (k *kubeInv) ClusterName(ctx context.Context) string {
	if k.clusterInfo != nil {
		info := k.clusterInfo.GetClusterInfo()
		if name, ok := info["name"]; ok && name != "" {
			return name
		}
	}

	// Try to get from environment
	if name := os.Getenv("CLUSTER_ID"); name != "" {
		return name
	}

	return "unknown"
}
