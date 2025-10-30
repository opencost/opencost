package kubemodel

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
