package kubemodel

type Cluster struct {
	UID      string
	Provider Provider
	Account  string
	Name     string
}
