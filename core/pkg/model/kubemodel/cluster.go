package kubemodel

import (
	"errors"
	"time"

	"github.com/opencost/opencost/core/pkg/model/shared"
)

// @bingen:generate:Cluster
type Cluster struct {
	UID      string          `json:"uid"`      // @bingen:field[version=1]
	Provider shared.Provider `json:"provider"` // @bingen:field[version=1]
	Account  string          `json:"account"`  // @bingen:field[version=1]
	Name     string          `json:"name"`     // @bingen:field[version=1]
	Region   string          `json:"region"`   // @bingen:field[version=2]
	Start    time.Time       `json:"start"`    // @bingen:field[version=1]
	End      time.Time       `json:"end"`      // @bingen:field[version=1]
}

func (kms *KubeModelSet) RegisterCluster(cluster *Cluster) error {
	if cluster.UID == "" {
		err := errors.New("RegisterCluster: uid is nil")
		kms.Error(err)
		return err
	}

	if kms.Cluster == nil {
		kms.Cluster = cluster
	} else if cluster.UID != kms.Cluster.UID {
		kms.Warnf("RegisterCluster(%s): attempting to change cluster UID from %s to %s", cluster.UID, kms.Cluster.UID, cluster.UID)
	} else {
		kms.Debugf("RegisterCluster(%s): cluster already registered", cluster.UID)
	}

	return nil
}
