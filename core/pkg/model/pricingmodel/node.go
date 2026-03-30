package pricingmodel

// @bingen:generate:NodeKey
type NodeKey struct {
	Provider string
	Region   string
	NodeType string
}

// @bingen:generate:NodePricing
type NodePricing struct {
	TotalHourlyRate float64
}
