package kubecost

import "time"

// CloudCostAggregateProperties unique property set for CloudCostAggregate within a window
type CloudCostAggregateProperties struct {
	Provider string
	Account  string
	Project  string
	Service  string
	Label    string
}

// CloudCostAggregate represents an aggregation of Billing Integration data on the properties listed
type CloudCostAggregate struct {
	Name              string
	Properties        CloudCostAggregateProperties
	KubernetesPercent float64
	Window            Window
	Start             time.Time
	End               time.Time
	Cost              float64
	Credit            float64
}

type CloudCostAggregateSet struct {
	CloudCosts map[string]*CloudCostAggregate
	Window     Window
	TotalCost  float64
}

func (ccas *CloudCostAggregateSet) GetWindow() Window {
	return ccas.Window
}
