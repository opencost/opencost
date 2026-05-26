package costmodel

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
)

// clampTimeToRange does not permit timestamps to exceed a given start, end
// range, inclusive of start and end times. For examples:
//
// If time is within (start, end) inclusive, return that time:
//
// >      S----T-------------E      => T
//
// If time is before start, return start:
//
// >   T  S------------------E      => S
//
// If time is after end, return end:
//
// >      S------------------E   T  => E
//
// Note: if this function encounters a "zero" time (either time.Zero or Unix
// timestamp 0) the time returned will be the given start time.
func clampTimeToRange(t time.Time, start, end time.Time) time.Time {
	if t.Before(start) {
		return start
	}

	if t.After(end) {
		return end
	}

	return t
}

func (cm *CostModel) ComputeAssets(start, end time.Time) (*opencost.AssetSet, error) {
	assetSet := opencost.NewAssetSet(start, end)

	nodeMap, err := cm.ClusterNodes(start, end)
	if err != nil {
		return nil, fmt.Errorf("error computing node assets for %s: %w", opencost.NewClosedWindow(start, end), err)
	}

	lbMap, err := cm.ClusterLoadBalancers(start, end)
	if err != nil {
		return nil, fmt.Errorf("error computing load balancer assets for %s: %w", opencost.NewClosedWindow(start, end), err)
	}

	diskMap, err := cm.ClusterDisks(start, end)
	if err != nil {
		return nil, fmt.Errorf("error computing disk assets for %s: %w", opencost.NewClosedWindow(start, end), err)
	}

	clusterManagement, err := cm.ClusterManagement(start, end)
	if err != nil {
		return nil, fmt.Errorf("error computing cluster management assets for %s: %w", opencost.NewClosedWindow(start, end), err)
	}

	for _, d := range diskMap {
		// Clamp the start and end fields to the start and end of the window.
		// In the case that start and end are missing (e.g. due to the "active
		// minutes" metric being absent), both times will be set to the start
		// of the window -- representing zero "runtime" within the window.
		s := clampTimeToRange(d.Start, start, end)
		e := clampTimeToRange(d.End, start, end)

		hours := e.Sub(s).Hours()

		disk := opencost.NewDisk(d.Name, d.Cluster, d.ProviderID, s, e, opencost.NewWindow(&start, &end))
		cm.PropertiesFromCluster(disk.Properties)
		disk.Cost = d.Cost
		disk.ByteHours = d.Bytes * hours
		if d.BytesUsedAvgPtr != nil {
			byteHours := *d.BytesUsedAvgPtr * hours
			disk.ByteHoursUsed = &byteHours
		}
		if d.BytesUsedMaxPtr != nil {
			usageMax := *d.BytesUsedMaxPtr
			disk.ByteUsageMax = &usageMax
		}

		if d.Local {
			disk.Local = 1.0
		}
		disk.Breakdown = &opencost.Breakdown{
			Idle:   d.Breakdown.Idle,
			System: d.Breakdown.System,
			User:   d.Breakdown.User,
			Other:  d.Breakdown.Other,
		}
		disk.StorageClass = d.StorageClass
		disk.VolumeName = d.VolumeName
		disk.ClaimName = d.ClaimName
		disk.ClaimNamespace = d.ClaimNamespace
		assetSet.Insert(disk, nil)
	}

	for _, lb := range lbMap {
		// Clamp the start and end fields to the start and end of the window.
		// In the case that start and end are missing (e.g. due to the "active
		// minutes" metric being absent), both times will be set to the start
		// of the window -- representing zero "runtime" within the window.
		s := clampTimeToRange(lb.Start, start, end)
		e := clampTimeToRange(lb.End, start, end)

		loadBalancer := opencost.NewLoadBalancer(lb.Name, lb.Cluster, lb.ProviderID, s, e, opencost.NewWindow(&start, &end), lb.Private, lb.Ip)
		cm.PropertiesFromCluster(loadBalancer.Properties)
		loadBalancer.Cost = lb.Cost

		assetSet.Insert(loadBalancer, nil)
	}

	for _, cman := range clusterManagement {
		cmAsset := opencost.NewClusterManagement(cman.Provisioner, cman.Cluster, opencost.NewClosedWindow(start, end))
		cm.PropertiesFromCluster(cmAsset.Properties)
		cmAsset.Cost = cman.Cost

		assetSet.Insert(cmAsset, nil)
	}

	for _, n := range nodeMap {
		// check label, to see if node from fargate, if so ignore.
		if n.Labels != nil {
			if value, ok := n.Labels["label_eks_amazonaws_com_compute_type"]; ok && value == "fargate" {
				continue
			}
		}

		// Clamp the start and end fields to the start and end of the window.
		// In the case that start and end are missing (e.g. due to the "active
		// minutes" metric being absent), both times will be set to the start
		// of the window -- representing zero "runtime" within the window.
		s := clampTimeToRange(n.Start, start, end)
		e := clampTimeToRange(n.End, start, end)

		hours := e.Sub(s).Hours()

		node := opencost.NewNode(n.Name, n.Cluster, n.ProviderID, s, e, opencost.NewWindow(&start, &end))
		cm.PropertiesFromCluster(node.Properties)
		node.NodeType = n.NodeType
		node.CPUCoreHours = n.CPUCores * hours
		node.RAMByteHours = n.RAMBytes * hours
		node.GPUHours = n.GPUCount * hours
		node.CPUBreakdown = &opencost.Breakdown{
			Idle:   n.CPUBreakdown.Idle,
			System: n.CPUBreakdown.System,
			User:   n.CPUBreakdown.User,
			Other:  n.CPUBreakdown.Other,
		}
		node.RAMBreakdown = &opencost.Breakdown{
			Idle:   n.RAMBreakdown.Idle,
			System: n.RAMBreakdown.System,
			User:   n.RAMBreakdown.User,
			Other:  n.RAMBreakdown.Other,
		}
		node.CPUCost = n.CPUCost
		node.GPUCost = n.GPUCost
		node.GPUCount = n.GPUCount
		node.RAMCost = n.RAMCost

		if n.Overhead != nil {
			node.Overhead = &opencost.NodeOverhead{
				RamOverheadFraction: n.Overhead.RamOverheadFraction,
				CpuOverheadFraction: n.Overhead.CpuOverheadFraction,
				OverheadCostFraction: ((n.Overhead.CpuOverheadFraction * n.CPUCost) +
					(n.Overhead.RamOverheadFraction * n.RAMCost)) / node.TotalCost(),
			}
		} else {
			node.Overhead = &opencost.NodeOverhead{}
		}
		node.Discount = n.Discount
		if n.Preemptible {
			node.Preemptible = 1.0
		}
		node.SetLabels(opencost.AssetLabels(n.Labels))
		assetSet.Insert(node, nil)
	}

	return assetSet, nil
}

func (cm *CostModel) ClusterDisks(start, end time.Time) (map[DiskIdentifier]*Disk, error) {
	return ClusterDisks(cm.DataSource, cm.Provider, start, end)
}

func (cm *CostModel) ClusterLoadBalancers(start, end time.Time) (map[LoadBalancerIdentifier]*LoadBalancer, error) {
	return ClusterLoadBalancers(cm.DataSource, start, end)
}

func (cm *CostModel) ClusterNodes(start, end time.Time) (map[NodeIdentifier]*Node, error) {
	return ClusterNodes(cm.DataSource, cm.Provider, start, end)
}

func (cm *CostModel) ClusterManagement(start, end time.Time) (map[ClusterManagementIdentifier]*ClusterManagementCost, error) {
	return ClusterManagement(cm.DataSource, start, end)
}

// propertiesFromCluster populates static cluster properties to individual asset properties
func (cm *CostModel) PropertiesFromCluster(props *opencost.AssetProperties) {
	// If properties does not have cluster value, do nothing
	if props.Cluster == "" {
		return
	}

	clusterMap := cm.ClusterMap.AsMap()
	ci, ok := clusterMap[props.Cluster]
	if !ok {
		log.Debugf("CostMode.PropertiesFromCluster: cluster '%s' was not found in ClusterMap", props.Cluster)
		return
	}

	props.Project = ci.Project
	props.Account = ci.Account
	props.Provider = ci.Provider
}
