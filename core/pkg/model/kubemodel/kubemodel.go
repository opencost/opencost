package kubemodel

import (
	"errors"
	"time"
)

type KubeModelSet struct {
	Metadata *KubeModelSetMetadata `json:"meta"`    // @bingen:field[version=1]
	Window   Window                `json:"window"`  // @bingen:field[version=1]
	Cluster  *Cluster              `json:"cluster"` // @bingen:field[version=1]
	idx      *kubeModelSetIndexes  // @bingen:field[ignore]
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
			namespaceByName:  map[string]*Namespace{},
			nodeByName:       map[string]*Node{},
			controllerByName: map[string]*Controller{},
			podByName:        map[string]*Pod{},
			serviceByName:    map[string]*Service{},
			pvcByName:        map[string]*PersistentVolumeClaim{},
			containerByName:  map[string]*Container{},
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
		ns := &Namespace{
			ID:             id,
			ClusterID:      kms.Cluster.ID,
			Name:           name,
			Controllers:    map[string]*Controller{},
			ResourceQuotas: map[string]*ResourceQuota{},
		}
		kms.Cluster.Namespaces[id] = ns

		// Index namespace by name for O(1) lookup
		if name != "" {
			kms.idx.namespaceByName[name] = ns
		}

		kms.Metadata.ObjectCount++
	}

	return nil
}

// GetNamespaceByName retrieves a namespace by its name using the index (O(1))
func (kms *KubeModelSet) GetNamespaceByName(name string) (*Namespace, bool) {
	if kms.idx == nil {
		return nil, false
	}

	ns, ok := kms.idx.namespaceByName[name]
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
		node := &Node{
			ID:               id,
			ClusterID:        kms.Cluster.ID,
			Name:             name,
			Pods:             map[string]*Pod{},
			EphemeralVolumes: map[string]*Volume{},
		}
		kms.Cluster.Nodes[id] = node

		// Index node by name for O(1) lookup
		if name != "" {
			kms.idx.nodeByName[name] = node
		}

		kms.Metadata.ObjectCount++
	}

	return nil
}

// GetNodeByName retrieves a node by its name using the index (O(1))
func (kms *KubeModelSet) GetNodeByName(name string) (*Node, bool) {
	if kms.idx == nil {
		return nil, false
	}

	node, ok := kms.idx.nodeByName[name]
	return node, ok
}

// RegisterController registers a controller in a namespace
func (kms *KubeModelSet) RegisterController(id, namespaceName, name string, kind ControllerKind) error {
	ns, ok := kms.GetNamespaceByName(namespaceName)
	if !ok {
		return errors.New("namespace not found: " + namespaceName)
	}

	if _, ok := ns.Controllers[id]; !ok {
		controller := &Controller{
			ID:          id,
			NamespaceID: ns.ID,
			Name:        name,
			Kind:        kind,
		}
		ns.Controllers[id] = controller

		// Index controller by namespace/name for O(1) lookup
		if name != "" {
			key := namespaceName + "/" + name
			kms.idx.controllerByName[key] = controller
		}

		kms.Metadata.ObjectCount++
	}

	return nil
}

// GetControllerByName retrieves a controller by namespace/name using the index (O(1))
func (kms *KubeModelSet) GetControllerByName(namespaceName, name string) (*Controller, bool) {
	if kms.idx == nil {
		return nil, false
	}

	key := namespaceName + "/" + name
	controller, ok := kms.idx.controllerByName[key]
	return controller, ok
}

// RegisterPod registers a pod in a node
func (kms *KubeModelSet) RegisterPod(id, nodeName, namespaceName, name string) error {
	node, ok := kms.GetNodeByName(nodeName)
	if !ok {
		return errors.New("node not found: " + nodeName)
	}

	ns, ok := kms.GetNamespaceByName(namespaceName)
	if !ok {
		return errors.New("namespace not found: " + namespaceName)
	}

	if _, ok := node.Pods[id]; !ok {
		pod := &Pod{
			ID:                     id,
			NodeID:                 node.ID,
			NamespaceID:            ns.ID,
			Name:                   name,
			Containers:             map[string]*Container{},
			AttachedDevices:        map[string]*Device{},
			PersistentVolumeClaims: map[string]*PersistentVolumeClaim{},
		}
		node.Pods[id] = pod

		// Index pod by namespace/name for O(1) lookup
		if name != "" && namespaceName != "" {
			key := namespaceName + "/" + name
			kms.idx.podByName[key] = pod
		}

		kms.Metadata.ObjectCount++
	}

	return nil
}

// GetPodByName retrieves a pod by namespace/name using the index (O(1))
func (kms *KubeModelSet) GetPodByName(namespaceName, name string) (*Pod, bool) {
	if kms.idx == nil {
		return nil, false
	}

	key := namespaceName + "/" + name
	pod, ok := kms.idx.podByName[key]
	return pod, ok
}

// RegisterContainer registers a container in a pod
func (kms *KubeModelSet) RegisterContainer(id, namespaceName, podName, name string) error {
	pod, ok := kms.GetPodByName(namespaceName, podName)
	if !ok {
		return errors.New("pod not found: " + namespaceName + "/" + podName)
	}

	if _, ok := pod.Containers[id]; !ok {
		container := &Container{
			PodID:   pod.ID,
			Name:    name,
			Volumes: map[string]*Volume{},
			Devices: map[string]*DeviceUsage{},
		}
		pod.Containers[id] = container

		// Index container by namespace/pod/name for O(1) lookup
		if name != "" {
			key := namespaceName + "/" + podName + "/" + name
			kms.idx.containerByName[key] = container
		}

		kms.Metadata.ObjectCount++
	}

	return nil
}

// GetContainerByName retrieves a container by namespace/pod/name using the index (O(1))
func (kms *KubeModelSet) GetContainerByName(namespaceName, podName, name string) (*Container, bool) {
	if kms.idx == nil {
		return nil, false
	}

	key := namespaceName + "/" + podName + "/" + name
	container, ok := kms.idx.containerByName[key]
	return container, ok
}

// RegisterPVC registers a persistent volume claim in a pod
func (kms *KubeModelSet) RegisterPVC(id, namespaceName, podName, name string) error {
	pod, ok := kms.GetPodByName(namespaceName, podName)
	if !ok {
		return errors.New("pod not found: " + namespaceName + "/" + podName)
	}

	if _, ok := pod.PersistentVolumeClaims[id]; !ok {
		ns, _ := kms.GetNamespaceByName(namespaceName)
		pvc := &PersistentVolumeClaim{
			ID:          id,
			NamespaceID: ns.ID,
			Name:        name,
		}
		pod.PersistentVolumeClaims[id] = pvc

		// Index PVC by namespace/name for O(1) lookup
		if name != "" {
			key := namespaceName + "/" + name
			kms.idx.pvcByName[key] = pvc
		}

		kms.Metadata.ObjectCount++
	}

	return nil
}

// GetPVCByName retrieves a PVC by namespace/name using the index (O(1))
func (kms *KubeModelSet) GetPVCByName(namespaceName, name string) (*PersistentVolumeClaim, bool) {
	if kms.idx == nil {
		return nil, false
	}

	key := namespaceName + "/" + name
	pvc, ok := kms.idx.pvcByName[key]
	return pvc, ok
}

// RegisterService registers a service/load balancer in the cluster
func (kms *KubeModelSet) RegisterService(id, namespaceName, name string) error {
	if kms.Cluster == nil {
		return errors.New("KubeModelSet missing Cluster")
	}

	ns, ok := kms.GetNamespaceByName(namespaceName)
	if !ok {
		return errors.New("namespace not found: " + namespaceName)
	}

	if kms.Cluster.LoadBalancers == nil {
		kms.Cluster.LoadBalancers = map[string]*Service{}
	}

	if _, ok := kms.Cluster.LoadBalancers[id]; !ok {
		service := &Service{
			ID:          id,
			ClusterID:   kms.Cluster.ID,
			NamespaceID: ns.ID,
			Name:        name,
		}
		kms.Cluster.LoadBalancers[id] = service

		// Index service by namespace/name for O(1) lookup
		if name != "" {
			key := namespaceName + "/" + name
			kms.idx.serviceByName[key] = service
		}

		kms.Metadata.ObjectCount++
	}

	return nil
}

// GetServiceByName retrieves a service by namespace/name using the index (O(1))
func (kms *KubeModelSet) GetServiceByName(namespaceName, name string) (*Service, bool) {
	if kms.idx == nil {
		return nil, false
	}

	key := namespaceName + "/" + name
	service, ok := kms.idx.serviceByName[key]
	return service, ok
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
	CreatedAt   time.Time           `json:"createdAt"`   // @bingen:field[version=1]
	CompletedAt time.Time           `json:"completedAt"` // @bingen:field[version=1]
	ObjectCount int                 `json:"objectCount"` // @bingen:field[version=1]
	Diagnostics []*DiagnosticResult `json:"diagnostics"` // @bingen:field[version=1]
}

type kubeModelSetIndexes struct {
	namespaceByName  map[string]*Namespace             // keyed by name
	nodeByName       map[string]*Node                  // keyed by name
	controllerByName map[string]*Controller            // keyed by namespace/name
	podByName        map[string]*Pod                   // keyed by namespace/name or node/name
	serviceByName    map[string]*Service               // keyed by namespace/name
	pvcByName        map[string]*PersistentVolumeClaim // keyed by namespace/name
	containerByName  map[string]*Container             // keyed by namespace/pod/name
}
