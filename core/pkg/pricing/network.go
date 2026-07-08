package pricing

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/model/shared"
)

type NetworkPricing struct {
	Properties NetworkPricingProperties `json:"properties" yaml:"properties"`
	Prices     Prices                   `json:"prices" yaml:"prices"`
}

func (sp *NetworkPricing) String() string {
	return sp.Properties.String() + "|" + sp.Prices.canonical()
}

type NetworkPricingProperties struct {
	Provider shared.Provider `json:"provider,omitempty" yaml:"provider,omitempty"`
	Start    *time.Time      `json:"start,omitempty" yaml:"start,omitempty"`
	End      *time.Time      `json:"end,omitempty" yaml:"end,omitempty"`
}

func (sp *NetworkPricingProperties) String() string {
	return fmt.Sprintf("%s:%s", sp.Provider, sp.timeKey())
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
