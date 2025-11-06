package kubemodel

import (
	"errors"
	"time"
)

type KubeModelSet struct {
	Metadata *KubeModelSetMetadata
	Window   Window
	Cluster  *Cluster
	idx      *kubeModelSetIndexes
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
		idx: &kubeModelSetIndexes{
			namespaceNameToID:  map[string]string{},
			nodeNameToID:       map[string]string{},
			controllerNameToID: map[string]string{},
			podNameToID:        map[string]string{},
			serviceNameToID:    map[string]string{},
			pvcNameToID:        map[string]string{},
		},
	}
}

// RegisterNamespace registers a namespace in the cluster
func (kms *KubeModelSet) RegisterNamespace(id string, name string) error {
	if kms.Cluster == nil {
		return errors.New("KubeModelSet missing Cluster")
	}

	if kms.Cluster.Namespaces == nil {
		kms.Cluster.Namespaces = map[string]*Namespace{}
	}

	if _, ok := kms.Cluster.Namespaces[id]; !ok {
		kms.Cluster.Namespaces[id] = &Namespace{
			ID:             id,
			ClusterID:      kms.Cluster.ID,
			Name:           name,
			Controllers:    map[string]*Controller{},
			ResourceQuotas: map[string]*ResourceQuota{},
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
	if kms.idx == nil || kms.Cluster == nil {
		return nil, false
	}

	id, ok := kms.idx.namespaceNameToID[name]
	if !ok {
		return nil, false
	}

	ns, ok := kms.Cluster.Namespaces[id]
	return ns, ok
}

// RegisterNode registers a node in the cluster
func (kms *KubeModelSet) RegisterNode(id string, name string) error {
	if kms.Cluster == nil {
		return errors.New("KubeModelSet missing Cluster")
	}

	if kms.Cluster.Nodes == nil {
		kms.Cluster.Nodes = map[string]*Node{}
	}

	if _, ok := kms.Cluster.Nodes[id]; !ok {
		kms.Cluster.Nodes[id] = &Node{
			ID:               id,
			ClusterID:        kms.Cluster.ID,
			Name:             name,
			Pods:             map[string]*Pod{},
			EphemeralVolumes: map[string]*Volume{},
		}

		// Index node name-to-ID for fast lookup
		if name != "" {
			kms.idx.nodeNameToID[name] = id
		}

		kms.Metadata.ObjectCount++
	}

	return nil
}

// GetNodeByName retrieves a node by its name using the index
func (kms *KubeModelSet) GetNodeByName(name string) (*Node, bool) {
	if kms.idx == nil || kms.Cluster == nil {
		return nil, false
	}

	id, ok := kms.idx.nodeNameToID[name]
	if !ok {
		return nil, false
	}

	node, ok := kms.Cluster.Nodes[id]
	return node, ok
}

// IsEmpty returns true if the KubeModelSet is nil, has no cluster, or contains no resources
func (kms *KubeModelSet) IsEmpty() bool {
	if kms == nil || kms.Cluster == nil {
		return true
	}

	// Check if all resource maps at cluster level are empty
	return len(kms.Cluster.Nodes) == 0 &&
		len(kms.Cluster.Namespaces) == 0 &&
		len(kms.Cluster.PersistentVolumes) == 0 &&
		len(kms.Cluster.LoadBalancers) == 0
}

// TODO: generate bingen codec
func (kms *KubeModelSet) MarshalBinary() ([]byte, error) {
	return nil, errors.New("not implemented")
}

type KubeModelSetMetadata struct {
	CreatedAt   time.Time
	CompletedAt time.Time
	ObjectCount int
	Diagnostics []*DiagnosticResult
}

type kubeModelSetIndexes struct {
	namespaceNameToID  map[string]string
	nodeNameToID       map[string]string
	controllerNameToID map[string]string // keyed by namespace/name
	podNameToID        map[string]string // keyed by namespace/name
	serviceNameToID    map[string]string // keyed by namespace/name
	pvcNameToID        map[string]string // keyed by namespace/name
}
