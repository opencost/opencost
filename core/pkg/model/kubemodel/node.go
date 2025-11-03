package kubemodel

// TODO complete
type Node struct {
	UID        string
	ClusterUID string
	Name       string
	Resources  ResourceQuantities
}
