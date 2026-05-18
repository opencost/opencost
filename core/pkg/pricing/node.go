package pricing

import "time"

type NodePricingProperties struct {
	Provider       Provider          `json:"provider,omitempty" yaml:"provider,omitempty"`
	Region         *string           `json:"region,omitempty" yaml:"region,omitempty"`
	InstanceFamily *string           `json:"instanceFamily,omitempty" yaml:"instanceFamily,omitempty"`
	InstanceType   *string           `json:"instanceType,omitempty" yaml:"instanceType,omitempty"`
	Provisioning   *ProvisioningType `json:"provisioning,omitempty" yaml:"provisioning,omitmpty"`
	Commitment     *CommitmentType   `json:"commitment,omitempty" yaml:"commitment,omitmpty"`
	Cluster        *string           `json:"cluster,omitempty" yaml:"cluster,omitempty"`
	ProviderID     *string           `json:"providerID,omitempty" yaml:"providerID,omitempty"`
	Labels         map[string]string `json:"labels,omitempty" yaml:"labels,omitempty"`
	Start          *time.Time        `json:"start,omitempty" yaml:"start,omitempty"`
	End            *time.Time        `json:"end,omitempty" yaml:"end,omitempty"`
}

type NodePricing struct {
	Properties    NodePricingProperties `json:"properties" yaml:"properties"`
	HourlyPrice   float64               `json:"hourlyPrice" yaml:"hourlyPrice"`
	CPUPercentage float64               `json:"cpuPercentage" yaml:"cpuPercentage"`
	RAMPercentage float64               `json:"ramPercentage" yaml:"ramPercentage"`
	GPUPercentage float64               `json:"gpuPercentage" yaml:"gpuPercentage"`
}

type NodePricingRequest struct {
	Filters NodePricingProperties
	Offset  int
	Limit   int
}
