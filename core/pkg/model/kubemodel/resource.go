package kubemodel

// Resource represents the type of compute resource
type Resource string // @bingen:generate[type=enum]

const (
	ResourceCPU     Resource = "cpu"     // @bingen:field[version=1]
	ResourceMemory  Resource = "memory"  // @bingen:field[version=1]
	ResourceGPU     Resource = "gpu"     // @bingen:field[version=1]
	ResourceStorage Resource = "storage" // @bingen:field[version=1]
)

// Unit represents the measurement unit for resources
type Unit string // @bingen:generate[type=enum]

const (
	// Basic units
	UnitMillicore Unit = "mCPU" // @bingen:field[version=1]
	UnitByte      Unit = "B"    // @bingen:field[version=1]
	UnitMi        Unit = "Mi"   // @bingen:field[version=1]
	UnitGB        Unit = "GB"   // @bingen:field[version=1]
	UnitGPU       Unit = "GPU"  // @bingen:field[version=1]

	// Time units
	UnitSecond Unit = "s" // @bingen:field[version=1]
	UnitMinute Unit = "m" // @bingen:field[version=1]
	UnitHour   Unit = "h" // @bingen:field[version=1]

	// Composite units (resource * time)
	UnitMillicoreHour Unit = "m-h"   // @bingen:field[version=1]
	UnitByteHour      Unit = "B-h"   // @bingen:field[version=1]
	UnitMiHour        Unit = "Mi-h"  // @bingen:field[version=1]
	UnitGBHour        Unit = "GB-h"  // @bingen:field[version=1]
	UnitGPUHour       Unit = "GPU-h" // @bingen:field[version=1]
)

// ResourceQuantity represents a measured quantity of a resource
type ResourceQuantity struct {
	Resource Resource `json:"resource"` // @bingen:field[version=1]
	Unit     Unit     `json:"unit"`     // @bingen:field[version=1]
	Values   Stats    `json:"values"`   // @bingen:field[version=1]
}

// ResourceQuantities is a map of resource types to their quantities
type ResourceQuantities map[Resource]ResourceQuantity // @bingen:generate[type=map]

// Set creates or updates a resource quantity with the given unit and stat type value
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
