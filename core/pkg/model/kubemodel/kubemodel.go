package kubemodel

import (
	"errors"
	"fmt"
	"time"
)

// @bingen:generate[stringtable]:KubeModelSet
type KubeModelSet struct {
	Metadata   *Metadata             `json:"meta"`                 // @bingen:field[version=1]
	Window     Window                `json:"window"`               // @bingen:field[version=1]
	Cluster    *Cluster              `json:"cluster"`              // @bingen:field[version=1]
	Namespaces map[string]*Namespace `json:"namespaces"`           // @bingen:field[version=1]
	Containers map[string]*Container `json:"containers,omitempty"` // @bingen:field[ignore]
	Owners     map[string]*Owner     `json:"owners,omitempty"`     // @bingen:field[ignore]
	Nodes      map[string]*Node      `json:"nodes,omitempty"`      // @bingen:field[ignore]
	Pods       map[string]*Pod       `json:"pods,omitempty"`       // @bingen:field[ignore]
	Services   map[string]*Service   `json:"services,omitempty"`   // @bingen:field[ignore]
	idx        *kubeModelSetIndexes  // @bingen:field[ignore]
}

func (kms *KubeModelSet) MarshalBinary() (data []byte, err error) {
	//TODO implement me
	panic("implement me")
}

func NewKubeModelSet(start time.Time, end time.Time) *KubeModelSet {
	return &KubeModelSet{
		Metadata: &Metadata{
			CreatedAt: time.Now().UTC(),
		},
		Window: Window{
			Start: start,
			End:   end,
		},
		Containers: map[string]*Container{},
		Owners:     map[string]*Owner{},
		Namespaces: map[string]*Namespace{},
		Nodes:      map[string]*Node{},
		Pods:       map[string]*Pod{},
		Services:   map[string]*Service{},
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
			UID:        id,
			ClusterUID: kms.Cluster.UID,
			Name:       name,
		}

		// Index namespace name-to-ID for fast lookup
		if name != "" {
			kms.idx.namespaceNameToID[name] = id
		}

		kms.Metadata.ObjectCount++
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

// IsEmpty returns true if the KubeModelSet is nil, has no cluster, or contains no resources
func (kms *KubeModelSet) IsEmpty() bool {
	if kms == nil || kms.Cluster == nil {
		return true
	}

	// Check if all resource maps are empty
	return len(kms.Containers) == 0 &&
		len(kms.Owners) == 0 &&
		len(kms.Namespaces) == 0 &&
		len(kms.Nodes) == 0 &&
		len(kms.Pods) == 0 &&
		len(kms.Services) == 0
}

func (kms *KubeModelSet) RegisterPod(id, name, namespace string) error {
	if _, ok := kms.Pods[id]; !ok {
		nsID, ok := kms.idx.namespaceNameToID[namespace]
		if !ok {
			return fmt.Errorf("KubeModelSet missing namespace '%s'", namespace)
		}

		kms.Pods[id] = &Pod{
			UID:          id,
			Name:         name,
			NamespaceUID: nsID,
		}

		kms.Metadata.ObjectCount++
	}

	return nil
}

func (kms *KubeModelSet) RegisterNode(id, name string) error {
	if _, ok := kms.Nodes[id]; !ok {
		if kms.Cluster == nil {
			return errors.New("KubeModelSet missing Cluster")
		}

		kms.Nodes[id] = &Node{
			UID:        id,
			ClusterUID: kms.Cluster.UID,
			Name:       name,
		}

		kms.Metadata.ObjectCount++
	}

	return nil
}

func (kms *KubeModelSet) RegisterOwner(id, name, namespace, kind string, isController bool) error {
	if _, ok := kms.Owners[id]; !ok {
		nsID, ok := kms.idx.namespaceNameToID[namespace]
		if !ok {
			return fmt.Errorf("KubeModelSet missing namespace '%s'", namespace)
		}

		kms.Owners[id] = &Owner{
			UID:        id,
			Name:       name,
			OwnerUID:   nsID,
			Kind:       OwnerKind(kind),
			Controller: isController,
		}

		kms.Metadata.ObjectCount++
	}

	return nil
}

func (kms *KubeModelSet) RegisterService(id, name, namespace string) error {
	if _, ok := kms.Services[id]; !ok {
		if kms.Cluster == nil {
			return errors.New("KubeModelSet missing Cluster")
		}

		nsID, ok := kms.idx.namespaceNameToID[namespace]
		if !ok {
			return fmt.Errorf("KubeModelSet missing namespace '%s'", namespace)
		}

		kms.Services[id] = &Service{
			UID:          id,
			ClusterUID:   kms.Cluster.UID,
			NamespaceUID: nsID,
			Name:         name,
		}

		kms.Metadata.ObjectCount++
	}

	return nil
}

func (kms *KubeModelSet) RegisterContainer(id, name, podID string) error {
	if _, ok := kms.Containers[id]; !ok {
		kms.Containers[id] = &Container{
			PodUID: podID,
			Name:   name,
		}

		kms.Metadata.ObjectCount++
	}

	return nil
}

type kubeModelSetIndexes struct {
	namespaceNameToID map[string]string
}
