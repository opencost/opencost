package kubemodel

import (
	"time"
)

// @bingen:generate:Cluster
type Cluster struct {
	UID      string    `json:"uid"`      // @bingen:field[version=1]
	Provider Provider  `json:"provider"` // @bingen:field[version=1]
	Account  string    `json:"account"`  // @bingen:field[version=1]
	Name     string    `json:"name"`     // @bingen:field[version=1]
	Start    time.Time `json:"start"`    // @bingen:field[version=1]
	End      time.Time `json:"end"`      // @bingen:field[version=1]
}

func (kms *KubeModelSet) RegisterCluster(uid string) {
	if uid == "" {
		kms.RegisterError("RegisterCluster: uid is nil for Cluster")
		return
	}

	if kms.Cluster == nil {
		kms.Cluster = &Cluster{UID: uid}
	}
}
