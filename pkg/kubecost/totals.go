package kubecost

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/kubecost/cost-model/pkg/log"
	"github.com/patrickmn/go-cache"
)

// TODO can we use ResourceTotals for all things or do we need specific structs
// like AllocationResourceTotals, AssetResourceTotals, etc.

type ResourceTotals struct {
	Start                 time.Time `json:"end"`
	End                   time.Time `json:"start"`
	Cluster               string    `json:"cluster,omitempty"`
	Node                  string    `json:"node,omitempty"`
	Count                 int       `json:"count"`
	AttachedVolumeCost    float64   `json:"attachedVolumeCost"`
	ClusterManagementCost float64   `json:"clusterManagementCost"`
	CPUCost               float64   `json:"cpuCost"`
	GPUCost               float64   `json:"gpuCost"`
	LoadBalancerCost      float64   `json:"loadBalancerCost"`
	NetworkCost           float64   `json:"networkCost"`
	PersistentVolumeCost  float64   `json:"persistentVolumeCost"`
	RAMCost               float64   `json:"ramCost"`
}

// TODO Do we need TotalAllocationCost? Maybe just different types?

func (rt *ResourceTotals) TotalCost() float64 {
	return rt.CPUCost + rt.GPUCost + rt.LoadBalancerCost + rt.AttachedVolumeCost + rt.ClusterManagementCost + rt.NetworkCost + rt.PersistentVolumeCost + rt.RAMCost
}

func ComputeResourceTotalsFromAllocations(as *AllocationSet, prop string) map[string]*ResourceTotals {
	rts := map[string]*ResourceTotals{}

	as.Each(func(name string, alloc *Allocation) {
		// Do not count idle or unmounted allocations
		if alloc.IsIdle() || alloc.IsUnmounted() {
			return
		}

		// Default to computing totals by Cluster, but allow override to use Node.
		key := alloc.Properties.Cluster
		if prop == AllocationNodeProp {
			key = alloc.Properties.Node
		}

		if rt, ok := rts[key]; ok {
			if rt.Start.After(alloc.Start) {
				rt.Start = alloc.Start
			}
			if rt.End.Before(alloc.End) {
				rt.End = alloc.End
			}

			if rt.Node != alloc.Properties.Node {
				rt.Node = ""
			}

			rt.Count++
			rt.CPUCost += alloc.CPUTotalCost()
			rt.GPUCost += alloc.GPUTotalCost()
			rt.LoadBalancerCost += alloc.LBTotalCost()
			rt.NetworkCost += alloc.NetworkTotalCost()
			rt.PersistentVolumeCost += alloc.PVCost()
			rt.RAMCost += alloc.RAMTotalCost()
		} else {
			rts[key] = &ResourceTotals{
				Start:                alloc.Start,
				End:                  alloc.End,
				Cluster:              alloc.Properties.Cluster,
				Node:                 alloc.Properties.Node,
				Count:                1,
				CPUCost:              alloc.CPUTotalCost(),
				GPUCost:              alloc.GPUTotalCost(),
				LoadBalancerCost:     alloc.LBTotalCost(),
				NetworkCost:          alloc.NetworkTotalCost(),
				PersistentVolumeCost: alloc.PVCost(),
				RAMCost:              alloc.RAMTotalCost(),
			}
		}
	})

	// TODO clean up
	total := 0.0
	for _, rt := range rts {
		total += rt.TotalCost()
	}
	log.Infof("ResourceTotals: recorded %.4f over %d %ss for %s", total, len(rts), prop, as.Window)

	return rts
}

func ComputeResourceTotalsFromAssets(as *AssetSet, prop AssetProperty) map[string]*ResourceTotals {
	rts := map[string]*ResourceTotals{}

	// TODO comment
	nodeNames := map[string]bool{}
	disks := map[string]*Disk{}

	as.Each(func(name string, asset Asset) {
		if node, ok := asset.(*Node); ok {
			// Default to computing totals by Cluster, but allow override to use Node.
			key := node.Properties().Cluster
			if prop == AssetNodeProp {
				key = node.Properties().Name
			}

			// Add node name to list of node names, but only if aggregating
			// by node. (These are to be used later for attached volumes.)
			if prop == AssetNodeProp {
				nodeNames[node.Properties().Name] = true
			}

			// adjustmentRate is used to scale resource costs proportionally
			// by the adjustment. This is necessary because we only get one
			// adjustment per Node, not one per-resource-per-Node.
			//
			// e.g. total cost = $90, adjustment = -$10 => 0.9
			// e.g. total cost = $150, adjustment = -$300 => 0.3333
			// e.g. total cost = $150, adjustment = $50 => 1.5
			adjustmentRate := 1.0
			if node.TotalCost()-node.Adjustment() == 0 {
				// If (totalCost - adjustment) is 0.0 then adjustment cancels
				// the entire node cost and we should make everything 0
				// without dividing by 0.
				adjustmentRate = 0.0
				log.DedupedWarningf(5, "ComputeResourceTotals: node cost adjusted to $0.00 for %s", node.Properties().Name)
			} else if node.Adjustment() != 0.0 {
				// adjustmentRate is the ratio of cost-with-adjustment (i.e. TotalCost)
				// to cost-without-adjustment (i.e. TotalCost - Adjustment).
				adjustmentRate = node.TotalCost() / (node.TotalCost() - node.Adjustment())
			}

			cpuCost := node.CPUCost * (1.0 - node.Discount) * adjustmentRate
			gpuCost := node.GPUCost * (1.0 - node.Discount) * adjustmentRate
			ramCost := node.RAMCost * (1.0 - node.Discount) * adjustmentRate

			if rt, ok := rts[key]; ok {
				if rt.Start.After(node.Start()) {
					rt.Start = node.Start()
				}
				if rt.End.Before(node.End()) {
					rt.End = node.End()
				}

				if rt.Node != node.Properties().Name {
					rt.Node = ""
				}

				rt.Count++
				rt.CPUCost += cpuCost
				rt.RAMCost += ramCost
				rt.GPUCost += gpuCost
			} else {
				rts[key] = &ResourceTotals{
					Start:   node.Start(),
					End:     node.End(),
					Cluster: node.Properties().Cluster,
					Node:    node.Properties().Name,
					Count:   1,
					CPUCost: cpuCost,
					RAMCost: ramCost,
					GPUCost: gpuCost,
				}
			}
		} else if disk, ok := asset.(*Disk); ok && prop == AssetNodeProp {
			// Only record attached disks when prop is Node
			disks[disk.Properties().Name] = disk
		} else if cm, ok := asset.(*ClusterManagement); ok && prop == AssetClusterProp {
			// TODO ?
			// Only record cluster management when prop is Cluster because we
			// can't break down ClusterManagement by node.
			key := cm.Properties().Cluster

			if _, ok := rts[key]; !ok {
				rts[key] = &ResourceTotals{
					Start:   cm.Start(),
					End:     cm.End(),
					Cluster: cm.Properties().Cluster,
				}
			}

			rts[key].Count++
			rts[key].ClusterManagementCost += cm.TotalCost()
		}
	})

	// Identify attached volumes as disks with names matching a node's name
	for name := range nodeNames {
		if disk, ok := disks[name]; ok {
			// Default to computing totals by Cluster, but allow override to use Node.
			key := disk.Properties().Cluster
			if prop == AssetNodeProp {
				key = disk.Properties().Name
			}

			if key == "" {
				// TODO ?
				log.Warningf("ResourceTotals: disk missing key: %s", disk.Properties().Name)
			}

			if _, ok := rts[key]; !ok {
				rts[key] = &ResourceTotals{
					Start:   disk.Start(),
					End:     disk.End(),
					Cluster: disk.Properties().Cluster,
					Node:    name,
				}
			}

			rts[key].Count++
			rts[key].AttachedVolumeCost += disk.TotalCost()
		}
	}

	// TODO clean up
	total := 0.0
	for _, rt := range rts {
		total += rt.TotalCost()
	}
	log.Infof("ResourceTotals: recorded %.4f over %d %ss for %s", total, len(rts), prop, as.Window)

	return rts
}

// ComputeIdleCoefficients returns the idle coefficients for CPU, GPU, and RAM
// (in that order) for the given resource costs and totals.
func ComputeIdleCoefficients(shareSplit, key string, cpuCost, gpuCost, ramCost float64, allocationTotals map[string]*ResourceTotals) (float64, float64, float64) {
	if shareSplit == ShareNone {
		return 0.0, 0.0, 0.0
	}

	if shareSplit != ShareEven {
		shareSplit = ShareWeighted
	}

	var cpuCoeff, gpuCoeff, ramCoeff float64

	if _, ok := allocationTotals[key]; !ok {
		return 0.0, 0.0, 0.0
	}

	if shareSplit == ShareEven {
		coeff := 1.0 / float64(allocationTotals[key].Count)
		return coeff, coeff, coeff
	}

	if allocationTotals[key].CPUCost > 0 {
		cpuCoeff = cpuCost / allocationTotals[key].CPUCost
	}

	if allocationTotals[key].GPUCost > 0 {
		gpuCoeff = cpuCost / allocationTotals[key].GPUCost
	}

	if allocationTotals[key].RAMCost > 0 {
		ramCoeff = ramCost / allocationTotals[key].RAMCost
	}

	return cpuCoeff, gpuCoeff, ramCoeff
}

type ResourceTotalsStore interface {
	GetResourceTotalsByCluster(start, end time.Time) map[string]*ResourceTotals
	GetResourceTotalsByNode(start, end time.Time) map[string]*ResourceTotals
	SetResourceTotalsByCluster(start, end time.Time, rts map[string]*ResourceTotals)
	SetResourceTotalsByNode(start, end time.Time, rts map[string]*ResourceTotals)
}

func UpdateResourceTotalsStoreFromAllocationSet(rts ResourceTotalsStore, as *AllocationSet) error {
	if rts == nil {
		return errors.New("cannot update resource totals from AllocationSet for nil ResourceTotalsStore")
	}

	if as == nil {
		return errors.New("cannot update resource totals from AllocationSet for nil AllocationSet")
	}

	start := *as.Window.Start()
	end := *as.Window.End()

	rtsByCluster := ComputeResourceTotalsFromAllocations(as, AllocationClusterProp)
	rts.SetResourceTotalsByCluster(start, end, rtsByCluster)

	rtsByNode := ComputeResourceTotalsFromAllocations(as, AllocationNodeProp)
	rts.SetResourceTotalsByNode(start, end, rtsByNode)

	log.Infof("ETL: Allocation: updated resource totals: %s", as.Window)

	return nil
}

func UpdateResourceTotalsStoreFromAssetSet(rts ResourceTotalsStore, as *AssetSet) error {
	if rts == nil {
		return errors.New("cannot update resource totals from AssetSet for nil ResourceTotalsStore")
	}

	if as == nil {
		return errors.New("cannot update resource totals from AssetSet for nil AssetSet")
	}

	start := *as.Window.Start()
	end := *as.Window.End()

	rtsByCluster := ComputeResourceTotalsFromAssets(as, AssetClusterProp)
	rts.SetResourceTotalsByCluster(start, end, rtsByCluster)

	rtsByNode := ComputeResourceTotalsFromAssets(as, AssetNodeProp)
	rts.SetResourceTotalsByNode(start, end, rtsByNode)

	log.Infof("ETL: Asset: updated resource totals: %s", as.Window)

	return nil
}

type MemoryResourceTotalsStore struct {
	byCluster *cache.Cache
	byNode    *cache.Cache
}

func NewResourceTotalsStore() *MemoryResourceTotalsStore {
	return &MemoryResourceTotalsStore{
		byCluster: cache.New(cache.NoExpiration, cache.NoExpiration),
		byNode:    cache.New(cache.NoExpiration, cache.NoExpiration),
	}
}

func (mrts *MemoryResourceTotalsStore) GetResourceTotalsByCluster(start time.Time, end time.Time) map[string]*ResourceTotals {
	k := storeKey(start, end)
	if raw, ok := mrts.byCluster.Get(k); ok {
		return raw.(map[string]*ResourceTotals)
	} else {
		return nil
	}
}

func (mrts *MemoryResourceTotalsStore) GetResourceTotalsByNode(start time.Time, end time.Time) map[string]*ResourceTotals {
	k := storeKey(start, end)
	if raw, ok := mrts.byNode.Get(k); ok {
		return raw.(map[string]*ResourceTotals)
	} else {
		return nil
	}
}

func (mrts *MemoryResourceTotalsStore) SetResourceTotalsByCluster(start time.Time, end time.Time, rts map[string]*ResourceTotals) {
	k := storeKey(start, end)
	mrts.byCluster.Set(k, rts, cache.NoExpiration)
}

func (mrts *MemoryResourceTotalsStore) SetResourceTotalsByNode(start time.Time, end time.Time, rts map[string]*ResourceTotals) {
	k := storeKey(start, end)
	mrts.byNode.Set(k, rts, cache.NoExpiration)
}

// storeKey creates a storage key based on start and end times
func storeKey(start, end time.Time) string {
	startStr := strconv.FormatInt(start.Unix(), 10)
	endStr := strconv.FormatInt(end.Unix(), 10)
	return fmt.Sprintf("%s-%s", startStr, endStr)
}
