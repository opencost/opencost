package kubemodel

import (
	"errors"
	"time"
)

type KubeModelSet struct {
	Metadata       *KubeModelSetMetadata
	Window         Window
	Cluster        *Cluster
	Namespaces     map[string]*Namespace
	ResourceQuotas map[string]*ResourceQuota
	idx            *kubeModelSetIndexes
}

func NewKubeModelSet(start, end time.Time) *KubeModelSet {
	return &KubeModelSet{
		Metadata: &KubeModelSetMetadata{
			CreatedAt: time.Now().UTC(),
		},
		Window: Window{
			Start: start,
			End:   end,
		},
		Namespaces:     map[string]*Namespace{},
		ResourceQuotas: map[string]*ResourceQuota{},
	}
}

func (kms *KubeModelSet) RegisterNamespace(uid string) error {
	if _, ok := kms.Namespaces[uid]; !ok {
		if kms.Cluster == nil {
			return errors.New("KubeModelSet missing Cluster")
		}

		kms.Namespaces[uid] = &Namespace{
			UID:        uid,
			ClusterUID: kms.Cluster.UID,
		}

		// TODO: index namespace name-to-UID
	}

	return nil
}

func (kms *KubeModelSet) RegisterResourceQuota(uid string) error {
	if _, ok := kms.ResourceQuotas[uid]; !ok {
		if kms.Cluster != nil {
			return errors.New("KubeModelSet missing Cluster")
		}

		kms.ResourceQuotas[uid] = &ResourceQuota{
			UID:          uid,
			NamespaceUID: kms.Cluster.UID,
		}
	}

	return nil
}

// TODO: determine what "IsEmpty()" should mean here
func (kms *KubeModelSet) IsEmpty() bool {
	return kms == nil || kms.Cluster == nil
}

// TODO: generate bingen codec
func (kms *KubeModelSet) MarshalBinary() ([]byte, error) {
	return nil, errors.New("not implemented")
}

type KubeModelSetMetadata struct {
	CreatedAt   time.Time
	ObjectCount int
	Errors      []error
	Warnings    []string
}

type kubeModelSetIndexes struct {
	namespaceToNamespaceUID map[string]string
}
