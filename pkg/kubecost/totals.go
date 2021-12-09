package kubecost

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/kubecost/cost-model/pkg/log"
	"github.com/patrickmn/go-cache"
)

// AllocationTotals represents aggregate costs of all Allocations for
// a given cluster or tuple of (cluster, node) between a given start and end
// time, where the costs are aggregated per-resource. AllocationTotals
// is designed to be used as a pre-computed intermediate data structure when
// contextual knowledge is required to carry out a task, but computing totals
// on-the-fly would be expensive; e.g. idle allocation; sharing coefficients
// for idle or shared resources, etc.
type AllocationTotals struct {
	Start                          time.Time `json:"start"`
	End                            time.Time `json:"end"`
	Cluster                        string    `json:"cluster"`
	Node                           string    `json:"node"`
	Count                          int       `json:"count"`
	CPUCost                        float64   `json:"cpuCost"`
	CPUCostAdjustment              float64   `json:"cpuCostAdjustment"`
	GPUCost                        float64   `json:"gpuCost"`
	GPUCostAdjustment              float64   `json:"gpuCostAdjustment"`
	LoadBalancerCost               float64   `json:"loadBalancerCost"`
	LoadBalancerCostAdjustment     float64   `json:"loadBalancerCostAdjustment"`
	NetworkCost                    float64   `json:"networkCost"`
	NetworkCostAdjustment          float64   `json:"networkCostAdjustment"`
	PersistentVolumeCost           float64   `json:"persistentVolumeCost"`
	PersistentVolumeCostAdjustment float64   `json:"persistentVolumeCostAdjustment"`
	RAMCost                        float64   `json:"ramCost"`
	RAMCostAdjustment              float64   `json:"ramCostAdjustment"`
}

// ClearAdjustments sets all adjustment fields to 0.0
func (art *AllocationTotals) ClearAdjustments() {
	art.CPUCostAdjustment = 0.0
	art.GPUCostAdjustment = 0.0
	art.RAMCostAdjustment = 0.0
}

// TotalCPUCost returns CPU cost with adjustment.
func (art *AllocationTotals) TotalCPUCost() float64 {
	return art.CPUCost + art.CPUCostAdjustment
}

// TotalGPUCost returns GPU cost with adjustment.
func (art *AllocationTotals) TotalGPUCost() float64 {
	return art.GPUCost + art.GPUCostAdjustment
}

// TotalRAMCost returns RAM cost with adjustment.
func (art *AllocationTotals) TotalRAMCost() float64 {
	return art.RAMCost + art.RAMCostAdjustment
}

// TotalCost returns the sum of all costs.
func (art *AllocationTotals) TotalCost() float64 {
	return art.TotalCPUCost() + art.TotalGPUCost() + art.LoadBalancerCost +
		art.NetworkCost + art.PersistentVolumeCost + art.TotalRAMCost()
}

// ComputeAllocationTotals totals the resource costs of the given AllocationSet
// using the given property, i.e. cluster or node, where "node" really means to
// use the fully-qualified (cluster, node) tuple.
func ComputeAllocationTotals(as *AllocationSet, prop string) map[string]*AllocationTotals {
	arts := map[string]*AllocationTotals{}

	as.Each(func(name string, alloc *Allocation) {
		// Do not count idle or unmounted allocations
		if alloc.IsIdle() || alloc.IsUnmounted() {
			return
		}

		// Default to computing totals by Cluster, but allow override to use Node.
		key := alloc.Properties.Cluster
		if prop == AllocationNodeProp {
			key = fmt.Sprintf("%s/%s", alloc.Properties.Cluster, alloc.Properties.Node)
		}

		if _, ok := arts[key]; !ok {
			arts[key] = &AllocationTotals{
				Start:   alloc.Start,
				End:     alloc.End,
				Cluster: alloc.Properties.Cluster,
				Node:    alloc.Properties.Node,
			}
		}

		if arts[key].Start.After(alloc.Start) {
			arts[key].Start = alloc.Start
		}
		if arts[key].End.Before(alloc.End) {
			arts[key].End = alloc.End
		}

		if arts[key].Node != alloc.Properties.Node {
			arts[key].Node = ""
		}

		arts[key].Count++
		arts[key].CPUCost += alloc.CPUCost
		arts[key].CPUCostAdjustment += alloc.CPUCostAdjustment
		arts[key].GPUCost += alloc.GPUCost
		arts[key].GPUCostAdjustment += alloc.GPUCostAdjustment
		arts[key].LoadBalancerCost += alloc.LBTotalCost()
		arts[key].NetworkCost += alloc.NetworkTotalCost()
		arts[key].PersistentVolumeCost += alloc.PVCost()
		arts[key].RAMCost += alloc.RAMCost
		arts[key].RAMCostAdjustment += alloc.RAMCostAdjustment
	})

	return arts
}

// AssetTotals represents aggregate costs of all Assets for a given
// cluster or tuple of (cluster, node) between a given start and end time,
// where the costs are aggregated per-resource. AssetTotals is designed
// to be used as a pre-computed intermediate data structure when contextual
// knowledge is required to carry out a task, but computing totals on-the-fly
// would be expensive; e.g. idle allocation, shared tenancy costs
type AssetTotals struct {
	Start                 time.Time `json:"start"`
	End                   time.Time `json:"end"`
	Cluster               string    `json:"cluster"`
	Node                  string    `json:"node"`
	Count                 int       `json:"count"`
	AttachedVolumeCost    float64   `json:"attachedVolumeCost"`
	ClusterManagementCost float64   `json:"clusterManagementCost"`
	CPUCost               float64   `json:"cpuCost"`
	CPUCostAdjustment     float64   `json:"cpuCostAdjustment"`
	GPUCost               float64   `json:"gpuCost"`
	GPUCostAdjustment     float64   `json:"gpuCostAdjustment"`
	PersistentVolumeCost  float64   `json:"persistentVolumeCost"`
	RAMCost               float64   `json:"ramCost"`
	RAMCostAdjustment     float64   `json:"ramCostAdjustment"`
}

// ClearAdjustments sets all adjustment fields to 0.0
func (art *AssetTotals) ClearAdjustments() {
	art.CPUCostAdjustment = 0.0
	art.GPUCostAdjustment = 0.0
	art.RAMCostAdjustment = 0.0
}

// TotalCPUCost returns CPU cost with adjustment.
func (art *AssetTotals) TotalCPUCost() float64 {
	return art.CPUCost + art.CPUCostAdjustment
}

// TotalGPUCost returns GPU cost with adjustment.
func (art *AssetTotals) TotalGPUCost() float64 {
	return art.GPUCost + art.GPUCostAdjustment
}

// TotalRAMCost returns RAM cost with adjustment.
func (art *AssetTotals) TotalRAMCost() float64 {
	return art.RAMCost + art.RAMCostAdjustment
}

// TotalCost returns the sum of all costs
func (art *AssetTotals) TotalCost() float64 {
	return art.AttachedVolumeCost + art.ClusterManagementCost + art.TotalCPUCost() +
		art.TotalGPUCost() + art.PersistentVolumeCost + art.TotalRAMCost()
}

// ComputeAssetTotals totals the resource costs of the given AssetSet,
// using the given property, i.e. cluster or node, where "node" really means to
// use the fully-qualified (cluster, node) tuple.
// NOTE: we're not capturing LoadBalancers here yet, but only because we don't
// yet need them. They could be added.
func ComputeAssetTotals(as *AssetSet, prop AssetProperty) map[string]*AssetTotals {
	arts := map[string]*AssetTotals{}

	// Attached disks are tracked by matching their name with the name of the
	// node, as is standard for attached disks.
	nodeNames := map[string]bool{}
	disks := map[string]*Disk{}

	as.Each(func(name string, asset Asset) {
		if node, ok := asset.(*Node); ok {
			// Default to computing totals by Cluster, but allow override to use Node.
			key := node.Properties().Cluster
			if prop == AssetNodeProp {
				key = fmt.Sprintf("%s/%s", node.Properties().Cluster, node.Properties().Name)
			}

			// Add node name to list of node names, but only if aggregating
			// by node. (These are to be used later for attached volumes.)
			nodeNames[key] = true

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
				log.DedupedWarningf(5, "ComputeTotals: node cost adjusted to $0.00 for %s", node.Properties().Name)
			} else if node.Adjustment() != 0.0 {
				// adjustmentRate is the ratio of cost-with-adjustment (i.e. TotalCost)
				// to cost-without-adjustment (i.e. TotalCost - Adjustment).
				adjustmentRate = node.TotalCost() / (node.TotalCost() - node.Adjustment())
			}

			totalCPUCost := node.CPUCost * (1.0 - node.Discount)
			cpuCost := totalCPUCost * adjustmentRate
			cpuCostAdjustment := totalCPUCost - cpuCost

			totalGPUCost := node.GPUCost * (1.0 - node.Discount)
			gpuCost := totalGPUCost * adjustmentRate
			gpuCostAdjustment := totalGPUCost - gpuCost

			totalRAMCost := node.RAMCost * (1.0 - node.Discount)
			ramCost := totalRAMCost * adjustmentRate
			ramCostAdjustment := totalRAMCost - ramCost

			if _, ok := arts[key]; !ok {
				arts[key] = &AssetTotals{
					Start:   node.Start(),
					End:     node.End(),
					Cluster: node.Properties().Cluster,
					Node:    node.Properties().Name,
				}
			}

			if arts[key].Start.After(node.Start()) {
				arts[key].Start = node.Start()
			}
			if arts[key].End.Before(node.End()) {
				arts[key].End = node.End()
			}

			if arts[key].Node != node.Properties().Name {
				arts[key].Node = ""
			}

			arts[key].Count++
			arts[key].CPUCost += cpuCost
			arts[key].CPUCostAdjustment += cpuCostAdjustment
			arts[key].RAMCost += ramCost
			arts[key].RAMCostAdjustment += ramCostAdjustment
			arts[key].GPUCost += gpuCost
			arts[key].GPUCostAdjustment += gpuCostAdjustment
		} else if disk, ok := asset.(*Disk); ok {
			key := fmt.Sprintf("%s/%s", disk.Properties().Cluster, disk.Properties().Name)
			disks[key] = disk
		} else if cm, ok := asset.(*ClusterManagement); ok && prop == AssetClusterProp {
			// Only record cluster management when prop is Cluster because we
			// can't break down ClusterManagement by node.
			key := cm.Properties().Cluster

			if _, ok := arts[key]; !ok {
				arts[key] = &AssetTotals{
					Start:   cm.Start(),
					End:     cm.End(),
					Cluster: cm.Properties().Cluster,
				}
			}

			arts[key].Count++
			arts[key].ClusterManagementCost += cm.TotalCost()
		}
	})

	// Identify attached volumes as disks with names matching a node's name
	for name := range nodeNames {
		if disk, ok := disks[name]; ok {
			// By default, the key will be the name, which is the tuple of
			// cluster/node. But if we're aggregating by cluster only, then
			// reset the key to just the cluster.
			key := name
			if prop == AssetClusterProp {
				key = disk.Properties().Cluster
			}

			if _, ok := arts[key]; !ok {
				arts[key] = &AssetTotals{
					Start:   disk.Start(),
					End:     disk.End(),
					Cluster: disk.Properties().Cluster,
				}

				if prop == AssetNodeProp {
					arts[key].Node = disk.Properties().Name
				}
			}

			arts[key].Count++
			arts[key].AttachedVolumeCost += disk.TotalCost()
		}
	}

	return arts
}

// ComputeIdleCoefficients returns the idle coefficients for CPU, GPU, and RAM
// (in that order) for the given resource costs and totals.
func ComputeIdleCoefficients(shareSplit, key string, cpuCost, gpuCost, ramCost float64, allocationTotals map[string]*AllocationTotals) (float64, float64, float64) {
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

// TotalsStore acts as both an AllocationTotalsStore and an
// AssetTotalsStore.
type TotalsStore interface {
	AllocationTotalsStore
	AssetTotalsStore
}

// AllocationTotalsStore allows for storing (i.e. setting and
// getting) AllocationTotals by cluster and by node.
type AllocationTotalsStore interface {
	GetAllocationTotalsByCluster(start, end time.Time) (map[string]*AllocationTotals, bool)
	GetAllocationTotalsByNode(start, end time.Time) (map[string]*AllocationTotals, bool)
	SetAllocationTotalsByCluster(start, end time.Time, rts map[string]*AllocationTotals)
	SetAllocationTotalsByNode(start, end time.Time, rts map[string]*AllocationTotals)
}

// UpdateAllocationTotalsStore updates an AllocationTotalsStore
// by totaling the given AllocationSet and saving the totals.
func UpdateAllocationTotalsStore(arts AllocationTotalsStore, as *AllocationSet) error {
	if arts == nil {
		return errors.New("cannot update nil AllocationTotalsStore")
	}

	if as == nil {
		return errors.New("cannot update AllocationTotalsStore from nil AllocationSet")
	}

	if as.Window.IsOpen() {
		return errors.New("cannot update AllocationTotalsStore from AllocationSet with open window")
	}

	start := *as.Window.Start()
	end := *as.Window.End()

	artsByCluster := ComputeAllocationTotals(as, AllocationClusterProp)
	arts.SetAllocationTotalsByCluster(start, end, artsByCluster)

	artsByNode := ComputeAllocationTotals(as, AllocationNodeProp)
	arts.SetAllocationTotalsByNode(start, end, artsByNode)

	log.Infof("ETL: Allocation: updated resource totals for %s", as.Window)

	return nil
}

// AssetTotalsStore allows for storing (i.e. setting and getting)
// AssetTotals by cluster and by node.
type AssetTotalsStore interface {
	GetAssetTotalsByCluster(start, end time.Time) (map[string]*AssetTotals, bool)
	GetAssetTotalsByNode(start, end time.Time) (map[string]*AssetTotals, bool)
	SetAssetTotalsByCluster(start, end time.Time, rts map[string]*AssetTotals)
	SetAssetTotalsByNode(start, end time.Time, rts map[string]*AssetTotals)
}

// UpdateAssetTotalsStore updates an AssetTotalsStore
// by totaling the given AssetSet and saving the totals.
func UpdateAssetTotalsStore(arts AssetTotalsStore, as *AssetSet) error {
	if arts == nil {
		return errors.New("cannot update nil AssetTotalsStore")
	}

	if as == nil {
		return errors.New("cannot update AssetTotalsStore from nil AssetSet")
	}

	if as.Window.IsOpen() {
		return errors.New("cannot update AssetTotalsStore from AssetSet with open window")
	}

	start := *as.Window.Start()
	end := *as.Window.End()

	artsByCluster := ComputeAssetTotals(as, AssetClusterProp)
	arts.SetAssetTotalsByCluster(start, end, artsByCluster)

	artsByNode := ComputeAssetTotals(as, AssetNodeProp)
	arts.SetAssetTotalsByNode(start, end, artsByNode)

	log.Infof("ETL: Asset: updated resource totals for %s", as.Window)

	return nil
}

// MemoryTotalsStore is an in-memory cache TotalsStore
type MemoryTotalsStore struct {
	allocTotalsByCluster *cache.Cache
	allocTotalsByNode    *cache.Cache
	assetTotalsByCluster *cache.Cache
	assetTotalsByNode    *cache.Cache
}

// NewMemoryTotalsStore instantiates a new MemoryTotalsStore,
// which is composed of four in-memory caches.
func NewMemoryTotalsStore() *MemoryTotalsStore {
	return &MemoryTotalsStore{
		allocTotalsByCluster: cache.New(cache.NoExpiration, cache.NoExpiration),
		allocTotalsByNode:    cache.New(cache.NoExpiration, cache.NoExpiration),
		assetTotalsByCluster: cache.New(cache.NoExpiration, cache.NoExpiration),
		assetTotalsByNode:    cache.New(cache.NoExpiration, cache.NoExpiration),
	}
}

// GetAllocationTotalsByCluster retrieves the AllocationTotals
// by cluster for the given start and end times.
func (mts *MemoryTotalsStore) GetAllocationTotalsByCluster(start time.Time, end time.Time) (map[string]*AllocationTotals, bool) {
	k := storeKey(start, end)
	if raw, ok := mts.allocTotalsByCluster.Get(k); ok {
		return raw.(map[string]*AllocationTotals), true
	} else {
		return map[string]*AllocationTotals{}, false
	}
}

// GetAllocationTotalsByNode retrieves the AllocationTotals
// by node for the given start and end times.
func (mts *MemoryTotalsStore) GetAllocationTotalsByNode(start time.Time, end time.Time) (map[string]*AllocationTotals, bool) {
	k := storeKey(start, end)
	if raw, ok := mts.allocTotalsByNode.Get(k); ok {
		return raw.(map[string]*AllocationTotals), true
	} else {
		return map[string]*AllocationTotals{}, false
	}
}

// SetAllocationTotalsByCluster set the per-cluster AllocationTotals
// to the given values for the given start and end times.
func (mts *MemoryTotalsStore) SetAllocationTotalsByCluster(start time.Time, end time.Time, arts map[string]*AllocationTotals) {
	k := storeKey(start, end)
	mts.allocTotalsByCluster.Set(k, arts, cache.NoExpiration)
}

// SetAllocationTotalsByNode set the per-node AllocationTotals
// to the given values for the given start and end times.
func (mts *MemoryTotalsStore) SetAllocationTotalsByNode(start time.Time, end time.Time, arts map[string]*AllocationTotals) {
	k := storeKey(start, end)
	mts.allocTotalsByNode.Set(k, arts, cache.NoExpiration)
}

// GetAssetTotalsByCluster retrieves the AssetTotals
// by cluster for the given start and end times.
func (mts *MemoryTotalsStore) GetAssetTotalsByCluster(start time.Time, end time.Time) (map[string]*AssetTotals, bool) {
	k := storeKey(start, end)
	if raw, ok := mts.assetTotalsByCluster.Get(k); ok {
		return raw.(map[string]*AssetTotals), true
	} else {
		return map[string]*AssetTotals{}, false
	}
}

// GetAssetTotalsByNode retrieves the AssetTotals
// by node for the given start and end times.
func (mts *MemoryTotalsStore) GetAssetTotalsByNode(start time.Time, end time.Time) (map[string]*AssetTotals, bool) {
	k := storeKey(start, end)
	if raw, ok := mts.assetTotalsByNode.Get(k); ok {
		return raw.(map[string]*AssetTotals), true
	} else {
		return map[string]*AssetTotals{}, false
	}
}

// SetAssetTotalsByCluster set the per-cluster AssetTotals
// to the given values for the given start and end times.
func (mts *MemoryTotalsStore) SetAssetTotalsByCluster(start time.Time, end time.Time, arts map[string]*AssetTotals) {
	k := storeKey(start, end)
	mts.assetTotalsByCluster.Set(k, arts, cache.NoExpiration)
}

// SetAssetTotalsByNode set the per-node AssetTotals
// to the given values for the given start and end times.
func (mts *MemoryTotalsStore) SetAssetTotalsByNode(start time.Time, end time.Time, arts map[string]*AssetTotals) {
	k := storeKey(start, end)
	mts.assetTotalsByNode.Set(k, arts, cache.NoExpiration)
}

// storeKey creates a storage key based on start and end times
func storeKey(start, end time.Time) string {
	startStr := strconv.FormatInt(start.Unix(), 10)
	endStr := strconv.FormatInt(end.Unix(), 10)
	return fmt.Sprintf("%s-%s", startStr, endStr)
}
