package kubemodel

import "strings"

// @bingen:generate:Provider
type Provider string

const (
	ProviderEmpty        Provider = ""
	ProviderAWS          Provider = "AWS"
	ProviderGCP          Provider = "GCP"
	ProviderAzure        Provider = "Azure"
	ProviderAlibaba      Provider = "Alibaba"
	ProviderDigitalOcean Provider = "DigitalOcean"
	ProviderOracle       Provider = "Oracle"
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
	default:
		return ProviderEmpty
	}
}
