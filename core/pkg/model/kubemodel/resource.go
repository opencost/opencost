package kubemodel

// @bingen:generate:Resource
type Resource string

const (
	ResourceCPU     Resource = "cpu"
	ResourceMemory  Resource = "memory"
	ResourceGPU     Resource = "gpu"
	ResourceStorage Resource = "storage"
)

// @bingen:generate:ResourceQuantity
type ResourceQuantity struct {
	Resource Resource `json:"resource"` // @bingen:field[version=1]
	Unit     Unit     `json:"unit"`     // @bingen:field[version=1]
	Values   Stats    `json:"values"`   // @bingen:field[version=1]
}

// @bingen:generate:ResourceQuantities
type ResourceQuantities map[Resource]ResourceQuantity

func (rqs ResourceQuantities) Set(resource Resource, unit Unit, statType StatType, value float64) {
	if _, ok := rqs[resource]; !ok {
		rqs[resource] = ResourceQuantity{
			Resource: resource,
			Unit:     unit,
			Values:   NewStats(),
		}
	}

	rqs[resource].Values[statType] = value
}
