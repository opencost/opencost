package kubemodel

import (
	"errors"
	"time"
)

type KubeModelSet struct {
	Metadata               *KubeModelSetMetadata
	Window                 Window
	Cluster                *Cluster
	Containers             map[string]*Container
	Controllers            map[string]*Controller
	Devices                map[string]*Device
	Namespaces             map[string]*Namespace
	Nodes                  map[string]*Node
	Pods                   map[string]*Pod
	PersistentVolumeClaims map[string]*PersistentVolumeClaim
	ResourceQuotas         map[string]*ResourceQuota
	Services               map[string]*Service
	Volumes                map[string]*Volume
	idx                    *kubeModelSetIndexes
}

func NewKubeModelSet(start time.Time, end time.Time) *KubeModelSet {
	return &KubeModelSet{
		Metadata: &KubeModelSetMetadata{
			CreatedAt: time.Now().UTC(),
		},
		Window: Window{
			Start: start,
			End:   end,
		},
		Containers:             map[string]*Container{},
		Controllers:            map[string]*Controller{},
		Devices:                map[string]*Device{},
		Namespaces:             map[string]*Namespace{},
		Nodes:                  map[string]*Node{},
		Pods:                   map[string]*Pod{},
		PersistentVolumeClaims: map[string]*PersistentVolumeClaim{},
		ResourceQuotas:         map[string]*ResourceQuota{},
		Services:               map[string]*Service{},
		Volumes:                map[string]*Volume{},
		idx: &kubeModelSetIndexes{
			namespaceNameToID: map[string]string{},
		},
	}
}

func (kms *KubeModelSet) RegisterNamespace(id string, name string) error {
	if _, ok := kms.Namespaces[id]; !ok {
		if kms.Cluster == nil {
			return errors.New("KubeModelSet missing Cluster")
		}

		kms.Namespaces[id] = &Namespace{
			ID:        id,
			ClusterID: kms.Cluster.ID,
			Name:      name,
		}

		// Index namespace name-to-ID for fast lookup
		if name != "" {
			kms.idx.namespaceNameToID[name] = id
		}
	}

	return nil
}

// GetNamespaceByName retrieves a namespace by its name using the index
func (kms *KubeModelSet) GetNamespaceByName(name string) (*Namespace, bool) {
	if kms.idx == nil {
		return nil, false
	}

	id, ok := kms.idx.namespaceNameToID[name]
	if !ok {
		return nil, false
	}

	ns, ok := kms.Namespaces[id]
	return ns, ok
}

func (kms *KubeModelSet) RegisterResourceQuota(uid string, namespaceUID string) error {
	if _, ok := kms.ResourceQuotas[uid]; !ok {
		if kms.Cluster == nil {
			return errors.New("KubeModelSet missing Cluster")
		}

		kms.ResourceQuotas[uid] = &ResourceQuota{
			UID:          uid,
			NamespaceUID: namespaceUID,
		}
	}

	return nil
}

// IsEmpty returns true if the KubeModelSet is nil, has no cluster, or contains no resources
func (kms *KubeModelSet) IsEmpty() bool {
	if kms == nil || kms.Cluster == nil {
		return true
	}

	// Check if all resource maps are empty
	return len(kms.Containers) == 0 &&
		len(kms.Controllers) == 0 &&
		len(kms.Devices) == 0 &&
		len(kms.Namespaces) == 0 &&
		len(kms.Nodes) == 0 &&
		len(kms.Pods) == 0 &&
		len(kms.PersistentVolumeClaims) == 0 &&
		len(kms.ResourceQuotas) == 0 &&
		len(kms.Services) == 0 &&
		len(kms.Volumes) == 0
}

// TODO: generate bingen codec
func (kms *KubeModelSet) MarshalBinary() ([]byte, error) {
	return nil, errors.New("not implemented")
}

type KubeModelSetMetadata struct {
	CreatedAt   time.Time
	ObjectCount int
	Diagnostics []*DiagnosticResult
}

type kubeModelSetIndexes struct {
	namespaceNameToID map[string]string
}
