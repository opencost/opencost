package models

import "github.com/opencost/opencost/core/pkg/clustercache"

// ServiceLoadBalancerPricingProvider is an optional interface that providers can implement
// to provide service-specific Load Balancer pricing (e.g. based on flavor/size annotations).
type ServiceLoadBalancerPricingProvider interface {
	ServiceLoadBalancerPricing(service *clustercache.Service) (*LoadBalancer, error)
}

// Network is the interface by which the provider and cost model communicate network egress prices.
// The provider will best-effort try to fill out this struct.
type Network struct {
	ZoneNetworkEgressCost     float64
	RegionNetworkEgressCost   float64
	InternetNetworkEgressCost float64
	NatGatewayEgressCost      float64
	NatGatewayIngressCost     float64
}

// LoadBalancer is the interface by which the provider and cost model communicate LoadBalancer prices.
// The provider will best-effort try to fill out this struct.
type LoadBalancer struct {
	IngressIPAddresses []string `json:"IngressIPAddresses"`
	Cost               float64  `json:"hourlyCost"`
}
