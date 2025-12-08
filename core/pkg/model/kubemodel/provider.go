package kubemodel

// @bingen:generate:Provider
type Provider string

const (
<<<<<<< HEAD
	ProviderEmpty        Provider = ""
	ProviderAWS          Provider = "AWS"
	ProviderGCP          Provider = "GCP"
	ProviderAzure        Provider = "Azure"
	ProviderAlibaba      Provider = "Alibaba"
	ProviderDigitalOcean Provider = "DigitalOcean"
	ProviderOracle       Provider = "Oracle"
)
=======
	ProviderAWS          Provider = "aws"
	ProviderGCP          Provider = "gcp"
	ProviderAzure        Provider = "azure"
	ProviderOnPremises   Provider = "on_premises"
	ProviderAlibaba      Provider = "alibaba"
	ProviderDigitalOcean Provider = "digitalocean"
	ProviderOracle       Provider = "oracle"
)
>>>>>>> 92af4761 (Introduce kubemodel with core Kubernetes resources (Cluster, Namespace, Node, Pod, Container, Owner, Service) (#3472))
