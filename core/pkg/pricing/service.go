package pricing

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/cloud"
)

type ServicePricing struct {
	Properties ServicePricingProperties `json:"properties" yaml:"properties"`
	Prices     Prices                   `json:"prices" yaml:"prices"`
}

func (sp *ServicePricing) String() string {
	return sp.Properties.String() + "|" + sp.Prices.canonical()
}

type ServicePricingProperties struct {
	Provider cloud.Provider `json:"provider,omitempty" yaml:"provider,omitempty"`
	Region   string         `json:"region,omitempty" yaml:"region,omitempty"`
	Start    *time.Time     `json:"start,omitempty" yaml:"start,omitempty"`
	End      *time.Time     `json:"end,omitempty" yaml:"end,omitempty"`
}

func (sp *ServicePricingProperties) String() string {
	return fmt.Sprintf("%s:%s:%s",
		sp.Provider,
		sp.Region,
		sp.timeKey(),
	)
}

func (sp *ServicePricingProperties) timeKey() string {
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
