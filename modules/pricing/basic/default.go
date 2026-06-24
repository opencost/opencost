package basic

import (
	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/pricing"
	"github.com/opencost/opencost/core/pkg/unit"
)

const DefaultClusterPricePerHour float64 = 0.0

const DefaultNetworkLocalPricePerGiB float64 = 0.0
const DefaultNetworkCrossZonePricePerGiB float64 = 0.01
const DefaultNetworkCrossRegionPricePerGiB float64 = 0.01
const DefaultNetworkInternetPricePerGiB float64 = 0.143
const DefaultNetworkNATPricePerGiB float64 = 0.045

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
			Properties: pricing.NetworkPricingProperties{
				TrafficDirection: kubemodel.TrafficDirectionEgress,
				TrafficType:      kubemodel.TrafficTypeLocal,
				IsNatGateway:     false,
			},
			Prices: pricing.Prices{
				pricing.ResourceNetworkTraffic: {
					Unit:  unit.GiB,
					Price: DefaultNetworkLocalPricePerGiB,
				},
			},
		},
		{
			Properties: pricing.NetworkPricingProperties{
				TrafficDirection: kubemodel.TrafficDirectionEgress,
				TrafficType:      kubemodel.TrafficTypeCrossZone,
				IsNatGateway:     false,
			},
			Prices: pricing.Prices{
				pricing.ResourceNetworkTraffic: {
					Unit:  unit.GiB,
					Price: DefaultNetworkCrossZonePricePerGiB,
				},
			},
		},
		{
			Properties: pricing.NetworkPricingProperties{
				TrafficDirection: kubemodel.TrafficDirectionEgress,
				TrafficType:      kubemodel.TrafficTypeCrossRegion,
				IsNatGateway:     false,
			},
			Prices: pricing.Prices{
				pricing.ResourceNetworkTraffic: {
					Unit:  unit.GiB,
					Price: DefaultNetworkCrossRegionPricePerGiB,
				},
			},
		},
		{
			Properties: pricing.NetworkPricingProperties{
				TrafficDirection: kubemodel.TrafficDirectionEgress,
				TrafficType:      kubemodel.TrafficTypeInternet,
				IsNatGateway:     false,
			},
			Prices: pricing.Prices{
				pricing.ResourceNetworkTraffic: {
					Unit:  unit.GiB,
					Price: DefaultNetworkInternetPricePerGiB,
				},
			},
		},
		{
			Properties: pricing.NetworkPricingProperties{
				TrafficDirection: kubemodel.TrafficDirectionEgress,
				TrafficType:      kubemodel.TrafficTypeInternet,
				IsNatGateway:     true,
			},
			Prices: pricing.Prices{
				pricing.ResourceNetworkTraffic: {
					Unit:  unit.GiB,
					Price: DefaultNetworkInternetPricePerGiB + DefaultNetworkNATPricePerGiB,
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
