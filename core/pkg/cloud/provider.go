package cloud

import "strings"

// TODO: reconsider "shared" as a package name
// TODO: for this file, maybe core/pkg/model/cloud?
// TODO: maybe even core/pkg/cloud?

type Provider string

const (
	ProviderEmpty        Provider = ""
	ProviderAWS          Provider = "AWS"
	ProviderGCP          Provider = "GCP"
	ProviderAzure        Provider = "Azure"
	ProviderAlibaba      Provider = "Alibaba"
	ProviderDigitalOcean Provider = "DigitalOcean"
	ProviderOracle       Provider = "Oracle"
	ProviderScaleway     Provider = "Scaleway"
	ProviderOTC          Provider = "OTC"
	ProviderOVH          Provider = "OVH"
	ProviderSTACKIT      Provider = "STACKIT"
	ProviderCustom       Provider = "Custom"
	ProviderCSV          Provider = "CSV"
)

// ParseProvider converts a string to a Provider type, performing case-insensitive matching.
// Returns ProviderEmpty if the provider string is not recognized.
func ParseProvider(provider string) Provider {
	switch strings.ToLower(provider) {
	case "aws", "amazon":
		return ProviderAWS
	case "gcp", "gce", "google":
		return ProviderGCP
	case "azure", "microsoft":
		return ProviderAzure
	case "alibaba":
		return ProviderAlibaba
	case "digitalocean", "do":
		return ProviderDigitalOcean
	case "oracle", "oci":
		return ProviderOracle
	case "scaleway", "scw", "kapsule":
		return ProviderScaleway
	case "otc":
		return ProviderOTC
	case "ovh", "ovhcloud", "ovh-mks":
		return ProviderOVH
	case "stackit", "ske":
		return ProviderSTACKIT
	case "custom":
		return ProviderCustom
	case "csv":
		return ProviderCSV
	default:
		return ProviderEmpty
	}
}
