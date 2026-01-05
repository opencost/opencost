package kubemodel

import (
	"errors"
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

func (kms *KubeModelSet) RegisterCluster(uid string) error {
	if uid == "" {
		err := errors.New("RegisterCluster: uid is nil")
		kms.Error(err)
		return err
	}

	if kms.Cluster == nil {
		kms.Cluster = &Cluster{UID: uid}
	} else if uid != kms.Cluster.UID {
		kms.Warnf("RegisterCluster(%s): attempting to change cluster UID from %s to %s", uid, kms.Cluster.UID, uid)
	} else {
		kms.Debugf("RegisterCluster(%s): cluster already registered", uid)
	}

	return nil
}
