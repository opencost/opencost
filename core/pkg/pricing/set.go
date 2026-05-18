package pricing

type PricingSet struct {
	Nodes   []*NodePricing   `json:"nodes" yaml:"nodes"`
	Volumes []*VolumePricing `json:"volumes" yaml:"volumes"`
}
