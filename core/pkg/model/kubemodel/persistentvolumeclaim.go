package kubemodel

// TODO complete
type PersistentVolumeClaim struct {
	UID                 string
	PersistentVolumeUID string
	Name                string
	Resources           ResourceQuantities
}
