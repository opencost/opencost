package kubemodel

type Namespace struct {
	ID          string
	ClusterID   string
	Name        string
	Labels      map[string]string
	Annotations map[string]string
}
