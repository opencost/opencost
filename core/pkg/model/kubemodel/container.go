package kubemodel

// TODO complete
type Container struct {
	UID          string
	PodUID       string
	Name         string
	Resources    ResourceQuantities
	VolumeMounts map[string]ResourceQuantity
}
