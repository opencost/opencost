package basic

import (
	"github.com/opencost/opencost/core/pkg/pricing"
	"github.com/opencost/opencost/core/pkg/unit"
)

const DefaultNodePricePerVCPUHour float64 = 0.031611
const DefaultNodePricePerRAMGiBHour float64 = 0.004237
const DefaultNodePricePerGPUHour float64 = 0.95
const DefaultNodePricePerLocalDiskGiBHour float64 = 0.0001096

const DefaultVolumePricePerGiBHour float64 = 0.00005479452

func GetDefaultPricingSet() *pricing.PricingSet {
	return &pricing.PricingSet{
		Nodes:   []*pricing.NodePricing{GetDefaultNodePricing()},
		Volumes: []*pricing.VolumePricing{GetDefaultVolumePricing()},
	}
}

func GetDefaultNodePricing() *pricing.NodePricing {
	return &pricing.NodePricing{
		Properties: pricing.NodePricingProperties{},
		Prices: pricing.Prices{
			unit.USD: []pricing.Price{
				{
					Currency: unit.USD,
					Unit:     unit.VCPUHour,
					Price:    DefaultNodePricePerVCPUHour,
				},
				{
					Currency: unit.USD,
					Unit:     unit.RAMGiBHour,
					Price:    DefaultNodePricePerRAMGiBHour,
				},
				{
					Currency: unit.USD,
					Unit:     unit.GPUHour,
					Price:    DefaultNodePricePerGPUHour,
				},
				{
					Currency: unit.USD,
					Unit:     unit.StorageGiBHour,
					Price:    DefaultNodePricePerLocalDiskGiBHour,
				},
			},
		},
	}
}

func GetDefaultVolumePricing() *pricing.VolumePricing {
	return &pricing.VolumePricing{
		Properties: pricing.VolumePricingProperties{},
		Prices: pricing.Prices{
			unit.USD: []pricing.Price{
				{
					Currency: unit.USD,
					Unit:     unit.StorageGiBHour,
					Price:    DefaultVolumePricePerGiBHour,
				},
			},
		},
	}
}
