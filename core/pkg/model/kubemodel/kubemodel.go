package kubemodel

import (
	"errors"
	"fmt"
	"time"
)

type KubeModelSet struct {
	Metadata       *Metadata                 `json:"meta"`           // @bingen:field[version=1]
	Window         Window                    `json:"window"`         // @bingen:field[version=1]
	Cluster        *Cluster                  `json:"cluster"`        // @bingen:field[version=1]
	Namespaces     map[string]*Namespace     `json:"namespaces"`     // @bingen:field[version=1]
	ResourceQuotas map[string]*ResourceQuota `json:"resourceQuotas"` // @bingen:field[version=1]
	idx            *index                    // @bingen:field[ignore]
}

func NewKubeModelSet(start, end time.Time) *KubeModelSet {
	index := &index{
		namespaceByName: map[string]*Namespace{},
	}

	return &KubeModelSet{
		Metadata: &Metadata{
			CreatedAt: time.Now().UTC(),
		},
		Window: Window{
			Start: start,
			End:   end,
		},
		Namespaces:     map[string]*Namespace{},
		ResourceQuotas: map[string]*ResourceQuota{},
		idx:            index,
	}
}

func (kms *KubeModelSet) RegisterNamespace(uid, name string) error {
	if _, ok := kms.Namespaces[uid]; !ok {
		if kms.Cluster == nil {
			return errors.New("KubeModelSet missing Cluster")
		}

		kms.Namespaces[uid] = &Namespace{
			UID:        uid,
			ClusterUID: kms.Cluster.UID,
			Name:       name,
		}

		kms.idx.namespaceByName[name] = kms.Namespaces[uid]

		kms.Metadata.ObjectCount++
	}

	return nil
}

func (kms *KubeModelSet) RegisterResourceQuota(uid, name, namespace string) error {
	if _, ok := kms.ResourceQuotas[uid]; !ok {
		if _, ok := kms.idx.namespaceByName[namespace]; !ok {
			return fmt.Errorf("KubeModelSet missing namespace '%s'", namespace)
		}

		kms.ResourceQuotas[uid] = &ResourceQuota{
			UID:          uid,
			Name:         name,
			NamespaceUID: kms.idx.namespaceByName[namespace].UID,
			Spec:         &ResourceQuotaSpec{Hard: &ResourceQuotaSpecHard{}},
			Status:       &ResourceQuotaStatus{Used: &ResourceQuotaStatusUsed{}},
		}

		kms.Metadata.ObjectCount++
	}

	return nil
}

func (kms *KubeModelSet) IsEmpty() bool {
	return kms == nil || kms.Cluster == nil || kms.Metadata.ObjectCount == 0
}

type index struct {
	namespaceByName map[string]*Namespace
}
