package kubemodel

import "time"

// TODO complete
// TODO how to handle PVCs?
// TODO how to handle attached devices?
// TODO how to handle labels and annos?
type Pod struct {
	UID                       string
	NamespaceUID              string
	NodeUID                   string
	OwnerUID                  string
	PersistentVolumeClaimUIDs map[string]struct{}
	Name                      string
	Labels                    map[string]string
	Annotations               map[string]string
	Start                     time.Time
	End                       time.Time
}
