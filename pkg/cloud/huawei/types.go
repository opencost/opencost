package huawei

// HuaweiNodeAttributes holds the live BSS on-demand pricing result for an ECS node.
type HuaweiNodeAttributes struct {
	Type  string // e.g. c7n.2xlarge.4
	OS    string // e.g. linux
	Price string // total hourly price for the instance, as returned by BSS
}

// HuaweiPVAttributes holds the live BSS on-demand pricing result for an EVS volume,
// normalized to a price per GB-hour.
type HuaweiPVAttributes struct {
	Type  string // EVS volume type, e.g. SATA/SAS/GPSSD/SSD
	Price string // price per GB-hour
}

// HuaweiLoadBalancerAttributes holds the live BSS on-demand pricing result for a
// Huawei Cloud Shared (Basic) ELB, keyed by cluster region rather than by a
// per-resource key (see loadBalancerFeatures).
type HuaweiLoadBalancerAttributes struct {
	Price string // hourly price for a Shared (Basic) ELB, as returned by BSS
}

// HuaweiPricing is either node, PV or load balancer pricing, mirroring the shape of
// the pricing data OpenCost keeps for other providers backed by an on-demand
// pricing API (see pkg/cloud/otc).
type HuaweiPricing struct {
	NodeAttributes         *HuaweiNodeAttributes
	PVAttributes           *HuaweiPVAttributes
	LoadBalancerAttributes *HuaweiLoadBalancerAttributes
}
