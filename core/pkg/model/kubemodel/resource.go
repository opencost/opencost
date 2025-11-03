package kubemodel

import "github.com/opencost/opencost/core/pkg/stats"

type Resource string

const (
	ResourceCPU     Resource = "cpu"
	ResourceMemory  Resource = "memory"
	ResourceGPU     Resource = "gpu"
	ResourceStorage Resource = "storage"
)

type Unit string

const (
	UnitCPUCore   = "vCPU"
	UnitCPUm      = "m"
	UnitByte      = "B"
	UnitMi        = "Mi"
	UnitGB        = "GB"
	UnitGPU       = "GPU"
	UnitTimeHr    = "hr"
	UnitCPUCoreHr = "vCPU-hr"
	UnitCPUmHr    = "m-hr"
	UnitBHr       = "B-hr"
	UnitMiHr      = "Mi-hr"
	UnitGBHr      = "GB-hr"
	UnitGPUHr     = "GPU-hr"
)

type ResourceQuantity struct {
	Resource Resource
	Unit     Unit
	Values   stats.Stats
}

type ResourceQuantities map[Resource]ResourceQuantity

func (rqs ResourceQuantities) Set(resource Resource, unit Unit, statType stats.StatType, value float64) {
	if _, ok := rqs[resource]; !ok {
		rqs[resource] = ResourceQuantity{
			Resource: resource,
			Unit:     unit,
			Values:   stats.NewStats(),
		}
	}

	rqs[resource].Values[statType] = value
}
