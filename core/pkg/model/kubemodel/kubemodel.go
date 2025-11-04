package kubemodel

import (
	"errors"
	"fmt"
	"time"
)

type KubeModelSet struct {
	Metadata               *KubeModelSetMetadata
	Window                 Window
	Cluster                *Cluster
	Containers             map[string]*Container
	Nodes                  map[string]*Node
	Namespaces             map[string]*Namespace
	PersistentVolumes      map[string]*PersistentVolume
	PersistentVolumeClaims map[string]*PersistentVolumeClaim
	Pods                   map[string]*Pod
	ResourceQuotas         map[string]*ResourceQuota
	indexes                *kubeModelSetIndexes
}

func NewKubeModelSet(start, end time.Time) *KubeModelSet {
	indexes := &kubeModelSetIndexes{
		namespaceToNamespaceUID: map[string]string{},
	}

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
		indexes:        indexes,
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

		kms.indexes.namespaceToNamespaceUID[name] = uid
	}

	return nil
}

func (kms *KubeModelSet) RegisterResourceQuota(uid, name, namespace string) error {
	if _, ok := kms.ResourceQuotas[uid]; !ok {
		if _, ok := kms.indexes.namespaceToNamespaceUID[namespace]; !ok {
			return fmt.Errorf("KubeModelSet missing NamespaceUID for namespace=%s", namespace)
		}

		kms.ResourceQuotas[uid] = &ResourceQuota{
			UID:          uid,
			Name:         name,
			NamespaceUID: kms.indexes.namespaceToNamespaceUID[namespace],
			Spec:         &ResourceQuotaSpec{Hard: &ResourceQuotaSpecHard{}},
			Status:       &ResourceQuotaStatus{Used: &ResourceQuotaStatusUsed{}},
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
	CompletedAt time.Time
	ObjectCount int
	Errors      []error
	Warnings    []string
}

type kubeModelSetIndexes struct {
	namespaceToNamespaceUID map[string]string
}
