package kubecost

import (
	"time"
)

// CloudCostAggregateProperties unique property set for CloudCostAggregate within a window
type CloudCostAggregateProperties struct {
	Provider   string
	Account    string
	Project    string
	Service    string
	LabelValue string
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

// TODO Integration here or on Properties?
// TODO LabelName here or on Properties?
type CloudCostAggregateSet struct {
	CloudCostAggregates map[string]*CloudCostAggregate
	Integration         string
	LabelName           string
	Window              Window
}

func (ccas *CloudCostAggregateSet) Clone() *CloudCostAggregateSet {
	// TODO
	return nil
}

func (ccas *CloudCostAggregateSet) IsEmpty() bool {
	// TODO
	return true
}

func (ccas *CloudCostAggregateSet) GetWindow() Window {
	return ccas.Window
}

type CloudCostAggregateSetRange struct {
	CloudCostAggregateSets []*CloudCostAggregateSet
	Step                   time.Duration
	Window                 Window
}
