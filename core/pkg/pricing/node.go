package pricing

import (
	"time"
)

type NodePricingProperties struct {
	Provider     Provider          `json:"provider,omitempty" yaml:"provider,omitempty"`
	Region       *string           `json:"region,omitempty" yaml:"region,omitempty"`
	InstanceType *string           `json:"instanceType,omitempty" yaml:"instanceType,omitempty"`
	Provisioning *ProvisioningType `json:"provisioning,omitempty" yaml:"provisioning,omitmpty"`
	Commitment   *CommitmentType   `json:"commitment,omitempty" yaml:"commitment,omitmpty"`
	Cluster      *string           `json:"cluster,omitempty" yaml:"cluster,omitempty"`
	ProviderID   *string           `json:"providerID,omitempty" yaml:"providerID,omitempty"`
	Labels       map[string]string `json:"labels,omitempty" yaml:"labels,omitempty"`
	Start        *time.Time        `json:"start,omitempty" yaml:"start,omitempty"`
	End          *time.Time        `json:"end,omitempty" yaml:"end,omitempty"`
}

type NodePricing struct {
	Properties NodePricingProperties `json:"properties" yaml:"properties"`
	Prices     []*Price              `json:"prices"`
}

/*

 1. CTE (scoped nodes)
 2. CTE (total-hourly pricing)
 3. CTE (per-resource pricing)
 4. CTE (per-resource default)                       Any
 5. CTE (total-hourly by instance type)              ECP
 6. CTE (per-resource by instance type)              ECP
 7. CTE (total-hourly by instance type, region)      AWS, Azure, ECP
 8. CTE (per-resource by instance type, region)      GCP, ECP
 9. CTE (total-hourly by labels)                     ECP
10. CTE (per-resource by labels)                     ECP
11. SELECT ... COALESCE(10, 9, 8, 7, 6, 5, 4)

*/
