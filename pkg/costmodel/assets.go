package costmodel

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/pkg/cloudcost"
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

	// Cloud costs (e.g. RDS/DCS/OBS billing) are optional: only present when a
	// CloudCostIntegration has been wired up via CloudCostQuerier. A failure here
	// is logged rather than propagated, so that an unavailable or misbehaving
	// CloudCostIntegration never prevents Node/Disk/LoadBalancer assets from being
	// computed.
	cloudAssets, err := cm.ClusterCloudCosts(start, end)
	if err != nil {
		log.Errorf("error computing cloud cost assets for %s: %s", opencost.NewClosedWindow(start, end), err)
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

	// The billing data covers the whole account, including the machines and
	// volumes this cluster runs on -- which the loops above have already
	// reported, priced from metrics. Inserting both would count those twice.
	nodePools := clusterNodePools(nodeMap)
	for _, ca := range cloudAssets {
		if isClusterCloudAsset(ca, nodePools) {
			log.Debugf("ComputeAssets: skipping %s asset %q: already reported as a cluster asset", ca.Type(), ca.Properties.Name)
			continue
		}
		assetSet.Insert(ca, nil)
	}

	return assetSet, nil
}

// clusterNodePools collects the names of the node pools the cluster's nodes
// belong to.
func clusterNodePools(nodeMap map[NodeIdentifier]*Node) map[string]struct{} {
	pools := map[string]struct{}{}
	for _, n := range nodeMap {
		if pool := n.Labels[opencost.HuaweiNodePoolLabel]; pool != "" {
			pools[pool] = struct{}{}
		}
	}
	return pools
}

// isClusterCloudAsset reports whether a billed resource is one the cost model
// already reports as a cluster asset, and so must not be inserted a second time
// from the billing side.
//
// The two sides identify a machine differently -- billing by its ECS instance
// ID, Kubernetes by its CCE node ID, which are unrelated -- so the match is by
// name: CCE names a node after the pool it belongs to
// ("cce-mlops-np-training-cpu-52qrp") and its disks after the node
// ("cce-mlops-np-training-cpu-52qrp-volume-0000"). A machine of the account
// that isn't in one of the cluster's pools (a standalone VM, say) has no
// cluster asset shadowing it and is kept.
//
// Only the resources the cost model actually prices are considered: the pools
// are known only when node metrics carry the CCE labels, so an empty pool set
// keeps everything, double-counting rather than dropping real costs.
func isClusterCloudAsset(ca *opencost.Cloud, nodePools map[string]struct{}) bool {
	switch ca.Type() {
	case opencost.ECSCloudAssetType, opencost.EVSCloudAssetType:
	default:
		return false
	}

	name := ca.Properties.Name
	for pool := range nodePools {
		if strings.HasPrefix(name, pool+"-") {
			return true
		}
	}
	return false
}

// ClusterCloudCosts converts CloudCost data already ingested via a registered
// CloudCostIntegration (see pkg/cloudcost) into Cloud assets covering [start, end).
// This is generic across every CloudCostIntegration implementation (AWS, Azure,
// GCP, Huawei, etc.) -- it reads whatever categorized CloudCost data is available
// for the window, regardless of which provider produced it. Returns (nil, nil) if
// no CloudCostQuerier has been configured.
func (cm *CostModel) ClusterCloudCosts(start, end time.Time) ([]*opencost.Cloud, error) {
	if cm.CloudCostQuerier == nil {
		return nil, nil
	}

	ccsr, err := cm.CloudCostQuerier.Query(context.Background(), cloudcost.QueryRequest{
		Start:      start,
		End:        end,
		Accumulate: opencost.AccumulateOptionAll,
		AggregateBy: []string{
			opencost.CloudCostProviderProp,
			opencost.CloudCostProviderIDProp,
			opencost.CloudCostServiceProp,
			opencost.CloudCostCategoryProp,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("error querying cloud costs for %s: %w", opencost.NewClosedWindow(start, end), err)
	}

	var cloudAssets []*opencost.Cloud
	for _, ccs := range ccsr.CloudCostSets {
		if ccs.Window.Start() == nil || ccs.Window.End() == nil {
			continue
		}

		// CloudCost is ingested daily and may cover a range different from the
		// requested [start, end); clamp to the requested window, matching the
		// pattern used for Disk/Node above.
		s := clampTimeToRange(*ccs.Window.Start(), start, end)
		e := clampTimeToRange(*ccs.Window.End(), start, end)

		for _, cc := range ccs.CloudCosts {
			if cc == nil || cc.Properties == nil {
				continue
			}

			cloudAsset := opencost.NewCloud(cc.Properties.Category, cc.Properties.ProviderID, s, e, opencost.NewWindow(&start, &end))
			cloudAsset.SetCloudType(cloudCostServiceToAssetType(cc.Properties.Provider, cc.Properties.Service))
			cloudAsset.Properties.Provider = cc.Properties.Provider
			cloudAsset.Properties.Service = cc.Properties.Service
			cloudAsset.Properties.Account = cc.Properties.AccountID
			// Name is what consumers of the assets API display for an asset.
			// Without it every Cloud asset of a service renders identically (as
			// the service itself) and the rows can't be told apart.
			cloudAsset.Properties.Name = cloudAssetName(cc.Properties)
			cloudAsset.SetLabels(cloudAssetLabels(cc.Properties))
			cloudAsset.Cost = cc.NetCost.Cost

			cloudAssets = append(cloudAssets, cloudAsset)
		}
	}

	return cloudAssets, nil
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

// cloudCostServiceToAssetType maps a CloudCost Service string (e.g. the BSS
// CLOUD_SERVICE_TYPE dimension value) to a specific AssetType for Cloud assets.
// This allows the Infra Assets dashboard to show RDS, DCS, OBS, etc. as
// separate categories instead of a single "Cloud" catch-all.
//
// The service naming is provider-specific, so only providers with a known
// mapping are sub-typed; every other provider keeps the generic Cloud type
// rather than risking a service name that coincidentally resembles a Huawei
// Cloud one being filed under the wrong type.
func cloudCostServiceToAssetType(provider, service string) opencost.AssetType {
	if provider == opencost.HuaweiProvider {
		return opencost.HuaweiServiceAssetType(service)
	}
	return opencost.CloudAssetType
}

// cloudAssetName is the resource's name in the provider's console when the
// billing data reports one, and its ID otherwise -- an ID is still far better
// than nothing, which is what an asset with no name falls back to displaying:
// the name of its service, identically for every resource of that service.
func cloudAssetName(props *opencost.CloudCostProperties) string {
	if name := props.Labels[opencost.AssetResourceNameLabel]; name != "" {
		return name
	}
	return props.ProviderID
}

// cloudAssetLabels carries the parts of a CloudCost that an Asset has no
// property for -- region, resource type and spec code -- over to the Cloud
// asset as labels, alongside whatever labels the billing data already had.
func cloudAssetLabels(props *opencost.CloudCostProperties) opencost.AssetLabels {
	labels := opencost.AssetLabels{}
	for k, v := range props.Labels {
		labels[k] = v
	}
	if props.RegionID != "" {
		labels[opencost.AssetRegionLabel] = props.RegionID
	}
	return labels
}
