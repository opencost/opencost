package kubemodel

type Namespace struct {
	UID         string
	ClusterUID  string
	Name        string
	Labels      map[string]string
	Annotations map[string]string
}
