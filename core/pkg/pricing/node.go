package pricing

import (
	"time"
)

type NodePricingProperties struct {
	Provider     Provider          `json:"provider,omitempty" yaml:"provider,omitempty"`
	Region       string            `json:"region,omitempty" yaml:"region,omitempty"`
	InstanceType string            `json:"instanceType,omitempty" yaml:"instanceType,omitempty"`
	Provisioning ProvisioningType  `json:"provisioning,omitempty" yaml:"provisioning,omitempty"`
	Commitment   CommitmentType    `json:"commitment,omitempty" yaml:"commitment,omitempty"`
	Cluster      string            `json:"cluster,omitempty" yaml:"cluster,omitempty"`
	ProviderID   string            `json:"providerID,omitempty" yaml:"providerID,omitempty"`
	Labels       map[string]string `json:"labels,omitempty" yaml:"labels,omitempty"`
	Start        *time.Time        `json:"start,omitempty" yaml:"start,omitempty"`
	End          *time.Time        `json:"end,omitempty" yaml:"end,omitempty"`
}

type NodePricing struct {
	Properties NodePricingProperties `json:"properties" yaml:"properties"`
	Prices     Prices                `json:"prices" yaml:"pricing"`
}
