package kubecost

// Default Version Set (uses -version flag passed) includes shared resources
// @bingen:generate:Window

// Asset Version Set: Includes Asset pipeline specific resources
// @bingen:set[name=Assets,version=16]
// @bingen:generate:Any
// @bingen:generate:Asset
// @bingen:generate:AssetLabels
// @bingen:generate:AssetProperties
// @bingen:generate:AssetProperty
// @bingen:generate:AssetPricingModels
// @bingen:generate[stringtable]:AssetSet
// @bingen:generate:AssetSetRange
// @bingen:generate:Breakdown
// @bingen:generate:Cloud
// @bingen:generate:ClusterManagement
// @bingen:generate:Disk
// @bingen:generate:LoadBalancer
// @bingen:generate:Network
// @bingen:generate:Node
// @bingen:generate:SharedAsset
// @bingen:end

// Allocation Version Set: Includes Allocation pipeline specific resources
// @bingen:set[name=Allocation,version=16]
// @bingen:generate:Allocation
// @bingen:generate[stringtable]:AllocationSet
// @bingen:generate:AllocationSetRange
// @bingen:generate:AllocationProperties
// @bingen:generate:AllocationProperty
// @bingen:generate:AllocationLabels
// @bingen:generate:AllocationAnnotations
// @bingen:generate:RawAllocationOnlyData
// @bingen:generate:PVAllocations
// @bingen:generate:PVKey
// @bingen:generate:PVAllocation
// @bingen:end

//go:generate bingen -package=kubecost -version=16 -buffer=github.com/kubecost/cost-model/pkg/util
