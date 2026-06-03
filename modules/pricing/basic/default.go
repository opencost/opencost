package basic

import (
	"github.com/opencost/opencost/core/pkg/pricing"
	"github.com/opencost/opencost/core/pkg/unit"
)

const DefaultNodePricePerVCPUHour float64 = 0.031611
const DefaultNodePricePerRAMGiBHour float64 = 0.004237
const DefaultNodePricePerGPUHour float64 = 0.95

// TODO: LocalStorageGBHour?

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
					Unit:     unit.GiBHour,
					Price:    DefaultNodePricePerRAMGiBHour,
				},
				{
					Currency: currency,
					Unit:     unit.GPUHour,
					Price:    DefaultNodePricePerGPUHour,
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
					Unit:     unit.GiBHour,
					Price:    DefaultVolumePricePerGiBHour,
				},
			},
		},
	}
}
