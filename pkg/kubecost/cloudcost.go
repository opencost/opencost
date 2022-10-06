package kubecost

import "time"

type CloudCostLabels map[string]string

type CloudCostProperties struct {
	ProviderID string          `json:"providerID,omitempty"`
	Provider   string          `json:"provider,omitempty"`
	Account    string          `json:"account,omitempty"`
	Project    string          `json:"project,omitempty"`
	Service    string          `json:"service,omitempty"`
	Labels     CloudCostLabels `json:"labels,omitempty"`
}

// CloudCost represents a CUR line item, identifying a cloud resource and
// its cost over some period of time.
type CloudCost struct {
	Name         string
	Properties   CloudCostProperties
	IsKubernetes bool
	Start        time.Time
	End          time.Time
	Cost         float64
	Credit       float64
}

type CloudCostSet struct {
	CloudCosts map[string]*CloudCostAggregate
	Window     Window
	TotalCost  float64
}
