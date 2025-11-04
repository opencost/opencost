package kubemodel

type Cluster struct {
	UID      string
	Provider Provider
	Account  string
	Name     string

	// NOTE: Alternate hierarchical structure
	Namespaces        map[string]*Namespace
	Nodes             map[string]*Node
	PersistentVolumes map[string]*PersistentVolume
}
