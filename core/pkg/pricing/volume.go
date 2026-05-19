package pricing

import "time"

type VolumePricingProperties struct {
	Provider   Provider          `json:"provider,omitempty" yaml:"provider,omitempty"`
	Region     string            `json:"region,omitempty" yaml:"region,omitempty"`
	VolumeType VolumeType        `json:"storageClass,omitempty" yaml:"storageClass,omitempty"`
	Cluster    string            `json:"cluster,omitempty" yaml:"cluster,omitempty"`
	ProviderID string            `json:"providerID,omitempty" yaml:"providerID,omitempty"`
	Labels     map[string]string `json:"labels,omitempty" yaml:"labels,omitempty"`
	Start      *time.Time        `json:"start,omitempty" yaml:"start,omitempty"`
	End        *time.Time        `json:"end,omitempty" yaml:"end,omitempty"`
}

type VolumePricing struct {
	Properties VolumePricingProperties `json:"properties" yaml:"properties"`
	Prices     []Price                 `json:"prices"`
}
