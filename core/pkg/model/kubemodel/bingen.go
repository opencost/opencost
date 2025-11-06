package kubemodel

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

// KubeModel Version Set: Includes KubeModel pipeline specific resources for hierarchical model
// @bingen:set[name=KubeModel,version=1]
// @bingen:generate[stringtable]:KubeModelSet
// @bingen:generate:Cluster
// @bingen:generate:KubeModelSetMetadata
// @bingen:generate:Namespace
// @bingen:generate:Node
// @bingen:generate:Pod
// @bingen:generate:Container
// @bingen:generate:Controller
// @bingen:generate:Device
// @bingen:generate:DeviceType
// @bingen:generate:DeviceUsage
// @bingen:generate:Service
// @bingen:generate:ServicePort
// @bingen:generate:Volume
// @bingen:generate:PersistentVolumeClaim
// @bingen:generate:Provider
// @bingen:generate:Resource
// @bingen:generate:ResourceQuantity
// @bingen:generate:ResourceQuantities
// @bingen:generate:ResourceQuota
// @bingen:generate:ResourceQuotaSpec
// @bingen:generate:ResourceQuotaSpecHard
// @bingen:generate:ResourceQuotaStatus
// @bingen:generate:ResourceQuotaStatusUsed
// @bingen:generate:Stats
// @bingen:generate:StatType
// @bingen:generate:Unit
// @bingen:generate:Window
// @bingen:generate:DiagnosticResult
// @bingen:end

//go:generate bingen -package=kubemodel -version=1 -buffer=github.com/opencost/opencost/core/pkg/util
