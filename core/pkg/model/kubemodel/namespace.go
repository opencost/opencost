package kubemodel

import (
	"fmt"
	"time"
)

// @bingen:generate:Namespace
type Namespace struct {
	UID         string            `json:"uid"`         // @bingen:field[version=1]
	ClusterUID  string            `json:"clusterUID"`  // @bingen:field[version=1]
	Name        string            `json:"name"`        // @bingen:field[version=1]
	Labels      map[string]string `json:"labels"`      // @bingen:field[version=1]
	Annotations map[string]string `json:"annotations"` // @bingen:field[version=1]
	Start       time.Time         `json:"start"`       // @bingen:field[version=1]
	End         time.Time         `json:"end"`         // @bingen:field[version=1]
}

func (kms *KubeModelSet) RegisterNamespace(uid, name string) error {
	if uid == "" {
		err := fmt.Errorf("UID is nil for Namespace '%s'", name)
		kms.Error(err)
		return err
	}

	if _, ok := kms.Namespaces[uid]; !ok {
		clusterUID := ""

		if kms.Cluster == nil {
			kms.Warnf("RegisterNamespace(%s, %s): Cluster is nil", uid, name)
		} else {
			clusterUID = kms.Cluster.UID
		}

		kms.Namespaces[uid] = &Namespace{
			UID:        uid,
			ClusterUID: clusterUID,
			Name:       name,
		}

		kms.idx.namespaceByName[name] = kms.Namespaces[uid]

		kms.Metadata.ObjectCount++
	}

	return nil
}
