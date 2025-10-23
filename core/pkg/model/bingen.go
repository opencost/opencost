package model

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

// Kubemodel Version Set: Includes Kubemodel pipeline specific resources
// @bingen:set[name=Kubemodel,version=1]
// @bingen:generate:Cluster
// @bingen:generate:Provider
// @bingen:generate:Resolution
// @bingen:generate:ControllerKind
// @bingen:generate:Window
// @bingen:generate:Container
// @bingen:generate:Controller
// @bingen:generate:DiagnosticResult
// @bingen:generate:GPUDevice
// @bingen:generate:GPUUsage
// @bingen:generate:Namespace
// @bingen:generate:ServicePort
// @bingen:generate:Service
// @bingen:generate:Node
// @bingen:generate:Pod
// @bingen:generate:Volume
// @bingen:generate:PersistentVolumeClaim
// @bingen:end

//go:generate bingen -package=model -version=1 -buffer=github.com/opencost/opencost/core/pkg/util
