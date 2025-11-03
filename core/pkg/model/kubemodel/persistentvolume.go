package kubemodel

// TODO complete
type PersistentVolume struct {
	UID        string
	ClusterUID string
	Name       string
	Capacity   ResourceQuantities
}
