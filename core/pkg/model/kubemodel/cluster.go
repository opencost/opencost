package kubemodel

import (
	"fmt"
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

func (c *Cluster) ValidateCluster(window Window) error {
	if c.UID == "" {
		err := fmt.Errorf("UID is missing for Cluster with name '%s'", c.Name)
		return err
	}

	if err := checkWindow(window, c.Start, c.End); err != nil {
		return err
	}

	return nil
}

func (kms *KubeModelSet) RegisterCluster(cluster *Cluster) error {
	err := cluster.ValidateCluster(kms.Window)
	if err != nil {
		kms.Errorf("RegisterCluster: invalid cluster: %w", err)
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
