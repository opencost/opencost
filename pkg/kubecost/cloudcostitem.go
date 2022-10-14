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

// CloudCostItem represents a CUR line item, identifying a cloud resource and
// its cost over some period of time.
type CloudCostItem struct {
	Name         string
	Properties   CloudCostProperties
	IsKubernetes bool
	Start        time.Time
	End          time.Time
	Cost         float64
	Credit       float64
}

type CloudCostItemSet struct {
	CloudCostItems map[string]*CloudCostItem
	Window         Window
}

func (ccis *CloudCostItemSet) Clone() *CloudCostItemSet {
	// TODO
	return nil
}

func (ccis *CloudCostItemSet) IsEmpty() bool {
	// TODO
	return true
}

func (ccis *CloudCostItemSet) GetWindow() Window {
	return ccis.Window
}

type CloudCostItemSetRange struct {
	CloudCostItemSets []*CloudCostItemSet
	Step              time.Duration
	Window            Window
}
