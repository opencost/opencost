package kubemodel

import "fmt"

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
	UnitGPU        = "gpu"
	UnitByte       = "B"
	UnitGB         = "GB"
	UnitTimeHr     = "hr"
	UnitCPUmHr     = "m-hr"
	UnitMemoryMiHr = "Mi-hr"
	UnitGPUHr      = "gpu-hr"
	UnitGBHr       = "GB-hr"
)

type ResourceQuantity struct {
	Resource Resource
	Unit     Unit
	Quantity float64
}

type ResourcePrice struct {
	Resource Resource
	Unit     Unit
	Price    float64
}

func (rq ResourceQuantity) ApplyPrice(rp ResourcePrice) (float64, error) {
	if rp.Resource != rq.Resource {
		return 0.0, fmt.Errorf("mismatched resources: %s != %s", rq.Unit, rp.Unit)
	}

	if rp.Unit != rq.Unit {
		return 0.0, fmt.Errorf("mismatched units: %s != %s", rq.Unit, rp.Unit)
	}

	return rq.Quantity * rp.Price, nil
}
