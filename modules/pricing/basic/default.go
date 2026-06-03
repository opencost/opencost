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

func GetDefaultNodePricing(currency unit.Currency) *pricing.NodePricing {
	return &pricing.NodePricing{
		Properties: pricing.NodePricingProperties{},
		Prices: pricing.Prices{
			currency: []pricing.Price{
				{
					Currency: currency,
					Unit:     unit.VCPUHour,
					Price:    DefaultNodePricePerVCPUHour,
				},
				{
					Currency: currency,
					Unit:     unit.RAMGiBHour,
					Price:    DefaultNodePricePerRAMGiBHour,
				},
				{
					Currency: currency,
					Unit:     unit.GPUHour,
					Price:    DefaultNodePricePerGPUHour,
				},
				{
					Currency: currency,
					Unit:     unit.StorageGiBHour,
					Price:    DefaultNodePricePerLocalDiskGiBHour,
				},
			},
		},
	}
}

func GetDefaultVolumePricing(currency unit.Currency) *pricing.VolumePricing {
	return &pricing.VolumePricing{
		Properties: pricing.VolumePricingProperties{},
		Prices: pricing.Prices{
			currency: []pricing.Price{
				{
					Currency: currency,
					Unit:     unit.StorageGiBHour,
					Price:    DefaultVolumePricePerGiBHour,
				},
			},
		},
	}
}
