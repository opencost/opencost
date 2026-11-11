package models

// LBKey represents metadata identifying a load balancer for pricing
type LBKey interface {
	ID() string
	Features() string
}

// CustomLBKey is a default implementation of LBKey
type CustomLBKey struct {
	LBID       string
	LBFeatures string
}

func (k *CustomLBKey) ID() string       { return k.LBID }
func (k *CustomLBKey) Features() string { return k.LBFeatures }

// NetworkKey represents metadata identifying a network resource for pricing
type NetworkKey interface {
	ID() string
	Features() string
}

// CustomNetworkKey is a default implementation of NetworkKey
type CustomNetworkKey struct {
	NetworkID       string
	NetworkFeatures string
}

func (k *CustomNetworkKey) ID() string       { return k.NetworkID }
func (k *CustomNetworkKey) Features() string { return k.NetworkFeatures }

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
