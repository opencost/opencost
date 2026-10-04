package pricing

type ProvisioningType string

const (
	ProvisioningOnDemand ProvisioningType = "on-demand"
	ProvisioningSpot     ProvisioningType = "spot"
)
