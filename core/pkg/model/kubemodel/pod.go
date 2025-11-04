package kubemodel

// TODO complete
type Pod struct {
	UID                       string
	NamespaceUID              string
	NodeUID                   string
	OwnerUID                  string
	PersistentVolumeClaimUIDs []string
	Name                      string

	// NOTE: Alternate hierarchical structure
	Containers             map[string]*Container
	PersistentVolumeClaims map[string]*PersistentVolumeClaim
}
