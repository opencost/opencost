package pricing

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/model/shared"
)

type NetworkPricing struct {
	Properties NetworkPricingProperties `json:"properties" yaml:"properties"`
	Prices     Prices                   `json:"prices" yaml:"pricing"`
}

func (sp *NetworkPricing) String() string {
	return sp.Properties.String() + "|" + sp.Prices.canonical()
}

type NetworkPricingProperties struct {
	Provider         shared.Provider            `json:"provider,omitempty" yaml:"provider,omitempty"`
	TrafficDirection kubemodel.TrafficDirection `json:"trafficDirection,omitempty" yaml:"trafficDirection,omitempty"`
	TrafficType      kubemodel.TrafficType      `json:"trafficType,omitempty" yaml:"trafficType,omitempty"`
	IsNatGateway     bool                       `json:"isNatGateway,omitempty" yaml:"isNatGateway,omitempty"`
	Start            *time.Time                 `json:"start,omitempty" yaml:"start,omitempty"`
	End              *time.Time                 `json:"end,omitempty" yaml:"end,omitempty"`
}

// TODO: precompute this somewhere along the way?
func (sp *NetworkPricingProperties) String() string {
	return fmt.Sprintf("%s:%s:%s:nat=%t:%s",
		sp.Provider,
		sp.TrafficDirection,
		sp.TrafficType,
		sp.IsNatGateway,
		sp.timeKey(),
	)
}

func (sp *NetworkPricingProperties) timeKey() string {
	s := "nil"
	e := "nil"

	if sp.Start != nil {
		s = sp.Start.UTC().Format(time.RFC3339Nano)
	}

	if sp.End != nil {
		e = sp.End.UTC().Format(time.RFC3339Nano)
	}

	return fmt.Sprintf("%s:%s", s, e)
}
