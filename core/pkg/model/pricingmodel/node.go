package pricingmodel

// @bingen:generate:NodeKey
type NodeKey struct {
	provider string
	region   string
	nodeType string
}

func NewNodeKey(provider, region, nodeType string) NodeKey {
	return NodeKey{provider: provider, region: region, nodeType: nodeType}
}

// @bingen:generate:NodePricing
type NodePricing struct {
	totalHourlyRate float64
}

func NewNodePricing(totalHourlyRate float64) NodePricing {
	return NodePricing{totalHourlyRate: totalHourlyRate}
}
