package kubemodel

// @bingen:generate:Provider
type Provider string

const (
	ProviderAWS          Provider = "aws"
	ProviderGCP          Provider = "gcp"
	ProviderAzure        Provider = "azure"
	ProviderOnPremises   Provider = "on_premises"
	ProviderAlibaba      Provider = "alibaba"
	ProviderDigitalOcean Provider = "digitalocean"
	ProviderOracle       Provider = "oracle"
)