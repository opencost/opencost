package kubemodel

// TODO complete
type Pod struct {
	UID                       string
	NamespaceUID              string
	NodeUID                   string
	OwnerUID                  string
	PersistentVolumeClaimUIDs []string
	Name                      string
}
