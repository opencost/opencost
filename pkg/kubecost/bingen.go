package kubecost

////////////////////////////////////////////////////////////////////////////////
// NOTE: If you add fields to _any_ struct that is serialized by bingen, please
// make sure to add those fields to the END of the struct definition. This is
// required for backwards-compatibility. So:
//
// type Foo struct {
//     ExistingField1 string
//     ExistingField2 int
// }
//
// becomes:
//
// type Foo struct {
//     ExistingField1 string
//     ExistingField2 int
//     NewField       float64 // @bingen: <- annotation ref: bingen README
// }
//
////////////////////////////////////////////////////////////////////////////////

// Default Version Set (uses -version flag passed) includes shared resources
// @bingen:generate:Window
// @bingen:generate:Coverage
// @bingen:generate:CoverageSet

// Asset Version Set: Includes Asset pipeline specific resources
// @bingen:set[name=Assets,version=19]
// @bingen:generate:Any
// @bingen:generate:Asset
// @bingen:generate:AssetLabels
// @bingen:generate:AssetProperties
// @bingen:generate:AssetProperty
// @bingen:generate[stringtable,preprocess,postprocess]:AssetSet
// @bingen:generate:AssetSetRange
// @bingen:generate:Breakdown
// @bingen:generate:Cloud
// @bingen:generate:ClusterManagement
// @bingen:generate:Disk
// @bingen:generate:LoadBalancer
// @bingen:generate:Network
// @bingen:generate:Node
// @bingen:generate:NodeOverhead
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

// @bingen:set[name=Audit,version=1]
// @bingen:generate:AllocationReconciliationAudit
// @bingen:generate:TotalAudit
// @bingen:generate:AggAudit
// @bingen:generate:AuditFloatResult
// @bingen:generate:AuditMissingValue
// @bingen:generate:AssetReconciliationAudit
// @bingen:generate:EqualityAudit
// @bingen:generate:AuditType
// @bingen:generate:AuditStatus
// @bingen:generate[stringtable]:AuditSet
// @bingen:generate:AuditSetRange
// @bingen:end

// @bingen:set[name=CloudCost,version=2]
// @bingen:generate:CloudCost
// @bingen:generate:CostMetric
// @bingen:generate[stringtable]:CloudCostSet
// @bingen:generate:CloudCostSetRange
// @bingen:generate:CloudCostProperties
// @bingen:generate:CloudCostLabels
// @bingen:end

//go:generate bingen -package=kubecost -version=17 -buffer=github.com/opencost/opencost/pkg/util
