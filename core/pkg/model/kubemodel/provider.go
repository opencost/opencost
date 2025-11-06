package kubemodel

// Provider represents the cloud provider or infrastructure type
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