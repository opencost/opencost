package kubemodel

// Cluster represents the top-level Kubernetes cluster
type Cluster struct {
	ID       string   `json:"id"`
	Provider Provider `json:"provider"`
	Account  string   `json:"account"`
	Name     string   `json:"name"`
}
