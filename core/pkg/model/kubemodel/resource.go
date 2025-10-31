package kubemodel

type Resource string

const (
	ResourceCPU    = "cpu"
	ResourceMemory = "memory"
	ResourceGPU    = "gpu"
)

type Unit string

const (
	UnitCPUm       = "m"
	UnitMemoryMi   = "Mi"
	UnitGPU        = "GPU"
	UnitByte       = "B"
	UnitGB         = "GB"
	UnitTimeHr     = "hr"
	UnitCPUmHr     = "m-hr"
	UnitMemoryMiHr = "Mi-hr"
	UnitGPUHr      = "GPU-hr"
	UnitGBHr       = "GB-hr"
)

type ResourceQuantity struct {
	Resource Resource
	Unit     Unit
	Quantity float64
}
