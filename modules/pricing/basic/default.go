package basic

import (
	"github.com/opencost/opencost/core/pkg/pricing"
	"github.com/opencost/opencost/core/pkg/unit"
)

const DefaultClusterPricePerHour float64 = 0.0

const DefaultNetworkLocalEgressPricePerGiB float64 = 0.0
const DefaultNetworkCrossZoneEgressPricePerGiB float64 = 0.01
const DefaultNetworkCrossRegionEgressPricePerGiB float64 = 0.01
const DefaultNetworkInternetEgressPricePerGiB float64 = 0.143
const DefaultNetworkNATGatewayEgressPricePerGiB float64 = 0.045
const DefaultNetworkNATGatewayIngressPricePerGiB float64 = 0.045

const DefaultNodePricePerVCPUHour float64 = 0.031611
const DefaultNodePricePerRAMGiBHour float64 = 0.004237
const DefaultNodePricePerGPUHour float64 = 0.95
const DefaultNodePricePerLocalDiskGiBHour float64 = 0.0001096

const DefaultPersistentVolumePricePerGiBHour float64 = 0.00005479452

const DefaultServicePricePerHour float64 = 0.025

func GetDefaultPricingSet() *pricing.PricingSet {
	return &pricing.PricingSet{
		ClusterPricing:          GetDefaultClusterPricing(),
		NetworkPricing:          GetDefaultNetworkPricing(),
		NodePricing:             GetDefaultNodePricing(),
		PersistentVolumePricing: GetDefaultPersistentVolumePricing(),
		ServicePricing:          GetDefaultServicePricing(),
	}
}

func GetDefaultClusterPricing() []*pricing.ClusterPricing {
	return []*pricing.ClusterPricing{
		{
			Properties: pricing.ClusterPricingProperties{},
			Prices: pricing.Prices{
				pricing.ResourceCluster: {
					Unit:  unit.Hour,
					Price: DefaultClusterPricePerHour,
				},
			},
		},
	}
}

func GetDefaultNetworkPricing() []*pricing.NetworkPricing {
	return []*pricing.NetworkPricing{
		{
			Properties: pricing.NetworkPricingProperties{},
			Prices: pricing.Prices{
				pricing.ResourceLocalEgress: {
					Unit:  unit.GiB,
					Price: DefaultNetworkLocalEgressPricePerGiB,
				},
				pricing.ResourceCrossZoneEgress: {
					Unit:  unit.GiB,
					Price: DefaultNetworkCrossZoneEgressPricePerGiB,
				},
				pricing.ResourceCrossRegionEgress: {
					Unit:  unit.GiB,
					Price: DefaultNetworkCrossRegionEgressPricePerGiB,
				},
				pricing.ResourceInternetEgress: {
					Unit:  unit.GiB,
					Price: DefaultNetworkInternetEgressPricePerGiB,
				},
				pricing.ResourceNATGatewayEgress: {
					Unit:  unit.GiB,
					Price: DefaultNetworkNATGatewayEgressPricePerGiB,
				},
				pricing.ResourceNATGatewayIngress: {
					Unit:  unit.GiB,
					Price: DefaultNetworkNATGatewayIngressPricePerGiB,
				},
			},
		},
	}
}

func GetDefaultNodePricing() []*pricing.NodePricing {
	return []*pricing.NodePricing{
		{
			Properties: pricing.NodePricingProperties{},
			Prices: pricing.Prices{
				pricing.ResourceCPU: {
					Unit:  unit.VCPUHour,
					Price: DefaultNodePricePerVCPUHour,
				},
				pricing.ResourceRAM: {
					Unit:  unit.GiBHour,
					Price: DefaultNodePricePerRAMGiBHour,
				},
				pricing.ResourceGPU: {
					Unit:  unit.GPUHour,
					Price: DefaultNodePricePerGPUHour,
				},
				pricing.ResourceStorage: {
					Unit:  unit.GiBHour,
					Price: DefaultNodePricePerLocalDiskGiBHour,
				},
			},
		},
	}
}

func GetDefaultPersistentVolumePricing() []*pricing.PersistentVolumePricing {
	return []*pricing.PersistentVolumePricing{
		{
			Properties: pricing.PersistentVolumePricingProperties{},
			Prices: pricing.Prices{
				pricing.ResourceStorage: {
					Unit:  unit.GiBHour,
					Price: DefaultPersistentVolumePricePerGiBHour,
				},
			},
		},
	}
}

func GetDefaultServicePricing() []*pricing.ServicePricing {
	return []*pricing.ServicePricing{
		{
			Properties: pricing.ServicePricingProperties{},
			Prices: pricing.Prices{
				pricing.ResourceService: {
					Unit:  unit.Hour,
					Price: DefaultServicePricePerHour,
				},
			},
		},
	}
}
