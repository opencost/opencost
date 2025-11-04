package kubemodel

import "github.com/opencost/opencost/core/pkg/stats"

type ContainerCosts struct {
	Container
	CPUCost     float64
	RAMCost     float64
	StorageCost float64
}

// ApplyContainerCosts is a simplified example of a consumer of the
// KubeModelSet data structure.
func ApplyContainerCosts(kms *KubeModelSet, pm PricingModel, out chan map[string]*ContainerCosts) {
	// 1. Option that uses the flat structure
	flatAlgorithm(kms, pm, out)

	// 2. Option that uses the cost-priority hierarchical structure
	hierarchyAlgorithm(kms, pm, out)
}

// O(C * avg(PVC))
// 42 lines
// 2 loops
// 3 max indentation
func flatAlgorithm(kms *KubeModelSet, pm PricingModel, out chan map[string]*ContainerCosts) {
	// O(1) -- slight advantage in being able to allocate memory for the whole set of Containers
	ccs := make(map[string]*ContainerCosts, len(kms.Containers))

	// O(C)
	for _, container := range kms.Containers {
		// O(1)
		cc := &ContainerCosts{Container: *container}

		// O(1)
		pod := kms.Pods[container.PodUID]

		// O(1)
		node := kms.Nodes[pod.NodeUID]

		// O(1) -- presumably
		nodePricing := pm.GetNodePricing(node)

		// O(1)
		cc.CPUCost = container.Resources[ResourceCPU].Values[stats.Val] * nodePricing[ResourceCPU].Price
		cc.RAMCost = container.Resources[ResourceMemory].Values[stats.Val] * nodePricing[ResourceMemory].Price

		// O(PVC)
		for pvcUID, storage := range container.VolumeMounts {
			// O(1)
			pvc := kms.PersistentVolumeClaims[pvcUID]

			// O(1)
			pv := kms.PersistentVolumes[pvc.PersistentVolumeUID]

			// O(1)
			pvPricing := pm.GetPersistentVolumePricing(pv)

			// O(1)
			cc.StorageCost += storage.Values[stats.Val] * pvPricing[ResourceStorage].Price
		}

		// O(1)
		ccs[container.UID] = cc
	}

	// O(1)
	out <- ccs
}

// O(N * avg(P) * avg(C) * avg(PVC)) == O(C * avg(PVC))
// 42 lines
// 4 loops
// 5 max indentation
func hierarchyAlgorithm(kms *KubeModelSet, pm PricingModel, out chan map[string]*ContainerCosts) {
	// O(1) -- total number of containers would have to be indexed / cached
	ccs := map[string]*ContainerCosts{}

	// O(N)
	for _, node := range kms.Cluster.Nodes {
		// O(avg(P))
		for _, pod := range node.Pods {
			// O(avg(C))
			for _, container := range pod.Containers {
				// O(1)
				cc := &ContainerCosts{Container: *container}

				// O(1) -- presumably
				nodePricing := pm.GetNodePricing(node)

				// O(1)
				cc.CPUCost = container.Resources[ResourceCPU].Values[stats.Val] * nodePricing[ResourceCPU].Price
				cc.RAMCost = container.Resources[ResourceMemory].Values[stats.Val] * nodePricing[ResourceMemory].Price

				// O(PVC) -- going to need to use the same access pattern as the
				// flat data structure, anyways, unless somehow PVCs / PVs will'
				// be nested within Containers (e.g. under VolumeMounts?)
				for pvcUID, storage := range container.VolumeMounts {
					// O(1)
					pvc := kms.PersistentVolumeClaims[pvcUID]

					// O(1)
					pv := kms.PersistentVolumes[pvc.PersistentVolumeUID]

					// O(1)
					pvPricing := pm.GetPersistentVolumePricing(pv)

					// O(1)
					cc.StorageCost += storage.Values[stats.Val] * pvPricing[ResourceStorage].Price
				}

				ccs[container.UID] = cc
			}
		}
	}

	out <- ccs
}

type PricingModel interface {
	GetNodePricing(node *Node) Pricing
	GetPersistentVolumePricing(pv *PersistentVolume) Pricing
}

type Pricing map[Resource]ResourcePricing

type ResourcePricing struct {
	Resource Resource
	Unit     Unit
	Price    float64
}
