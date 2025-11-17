package kubemodel

import (
	"errors"
	"fmt"
	"time"
)

// @bingen:generate[stringtable]:KubeModelSet
type KubeModelSet struct {
	Metadata               *Metadata                         `json:"meta"`                  // @bingen:field[version=1]
	Window                 Window                            `json:"window"`                // @bingen:field[version=1]
	Cluster                *Cluster                          `json:"cluster"`               // @bingen:field[version=1]
	Namespaces             map[string]*Namespace             `json:"namespaces"`            // @bingen:field[version=1]
	ResourceQuotas         map[string]*ResourceQuota         `json:"resourceQuotas"`        // @bingen:field[version=1]
	Containers             map[string]*Container             `json:"containers,omitempty"`  // @bingen:field[version=1]
	Controllers            map[string]*Controller            `json:"controllers,omitempty"` // @bingen:field[version=1]
	GPUDevices             map[string]*GPUDevice             `json:"gpuDevices,omitempty"`  // @bingen:field[version=1]
	GPUUsages              map[string]*GPUUsage              `json:"gpuUsages,omitempty"`   // @bingen:field[version=1]
	Nodes                  map[string]*Node                  `json:"nodes,omitempty"`       // @bingen:field[version=1]
	Pods                   map[string]*Pod                   `json:"pods,omitempty"`        // @bingen:field[version=1]
	PersistentVolumeClaims map[string]*PersistentVolumeClaim `json:"pvcs,omitempty"`        // @bingen:field[version=1]
	Services               map[string]*Service               `json:"services,omitempty"`    // @bingen:field[version=1]
	Volumes                map[string]*Volume                `json:"volumes,omitempty"`     // @bingen:field[version=1]
	idx                    *kubeModelSetIndexes              // @bingen:field[ignore]
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
		Containers:             map[string]*Container{},
		Controllers:            map[string]*Controller{},
		GPUDevices:             map[string]*GPUDevice{},
		GPUUsages:              map[string]*GPUUsage{},
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
			UID:        id,
			ClusterUID: kms.Cluster.UID,
			Name:       name,
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

// IsEmpty returns true if the KubeModelSet is nil, has no cluster, or contains no resources
func (kms *KubeModelSet) IsEmpty() bool {
	if kms == nil || kms.Cluster == nil {
		return true
	}

	// Check if all resource maps are empty
	return len(kms.Containers) == 0 &&
		len(kms.Controllers) == 0 &&
		len(kms.GPUDevices) == 0 &&
		len(kms.GPUUsages) == 0 &&
		len(kms.Namespaces) == 0 &&
		len(kms.Nodes) == 0 &&
		len(kms.Pods) == 0 &&
		len(kms.PersistentVolumeClaims) == 0 &&
		len(kms.ResourceQuotas) == 0 &&
		len(kms.Services) == 0 &&
		len(kms.Volumes) == 0
}

func (kms *KubeModelSet) RegisterResourceQuota(uid, name, namespace string) error {
	if _, ok := kms.ResourceQuotas[uid]; !ok {
		if _, ok := kms.idx.namespaceNameToID[namespace]; !ok {
			return fmt.Errorf("KubeModelSet missing namespace '%s'", namespace)
		}

		kms.ResourceQuotas[uid] = &ResourceQuota{
			UID:          uid,
			Name:         name,
			NamespaceUID: kms.idx.namespaceNameToID[namespace],
			Spec:         &ResourceQuotaSpec{Hard: &ResourceQuotaSpecHard{}},
			Status:       &ResourceQuotaStatus{Used: &ResourceQuotaStatusUsed{}},
		}

		kms.Metadata.ObjectCount++
	}

	return nil
}

func (kms *KubeModelSet) RegisterPod(id, name, namespace string) error {
	if _, ok := kms.Pods[id]; !ok {
		nsID, ok := kms.idx.namespaceNameToID[namespace]
		if !ok {
			return fmt.Errorf("KubeModelSet missing namespace '%s'", namespace)
		}

		kms.Pods[id] = &Pod{
			ID:          id,
			Name:        name,
			NamespaceID: nsID,
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
			ID:        id,
			ClusterID: kms.Cluster.UID,
			Name:      name,
		}

		kms.Metadata.ObjectCount++
	}

	return nil
}

func (kms *KubeModelSet) RegisterController(id, name, namespace, kind string) error {
	if _, ok := kms.Controllers[id]; !ok {
		nsID, ok := kms.idx.namespaceNameToID[namespace]
		if !ok {
			return fmt.Errorf("KubeModelSet missing namespace '%s'", namespace)
		}

		kms.Controllers[id] = &Controller{
			ID:          id,
			Name:        name,
			NamespaceID: nsID,
			Kind:        ControllerKind(kind),
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
			ID:          id,
			ClusterID:   kms.Cluster.UID,
			NamespaceID: nsID,
			Name:        name,
		}

		kms.Metadata.ObjectCount++
	}

	return nil
}

func (kms *KubeModelSet) RegisterPVC(id, name, namespace string) error {
	if _, ok := kms.PersistentVolumeClaims[id]; !ok {
		nsID, ok := kms.idx.namespaceNameToID[namespace]
		if !ok {
			return fmt.Errorf("KubeModelSet missing namespace '%s'", namespace)
		}

		kms.PersistentVolumeClaims[id] = &PersistentVolumeClaim{
			ID:          id,
			Name:        name,
			NamespaceID: nsID,
		}

		kms.Metadata.ObjectCount++
	}

	return nil
}

func (kms *KubeModelSet) RegisterVolume(id, name string) error {
	if _, ok := kms.Volumes[id]; !ok {
		if kms.Cluster == nil {
			return errors.New("KubeModelSet missing Cluster")
		}

		kms.Volumes[id] = &Volume{
			ID:        id,
			ClusterID: kms.Cluster.UID,
			Name:      name,
		}

		kms.Metadata.ObjectCount++
	}

	return nil
}

func (kms *KubeModelSet) RegisterContainer(id, name, podID string) error {
	if _, ok := kms.Containers[id]; !ok {
		kms.Containers[id] = &Container{
			PodID: podID,
			Name:  name,
		}

		kms.Metadata.ObjectCount++
	}

	return nil
}

func (kms *KubeModelSet) RegisterGPUDevice(id, nodeID string) error {
	if _, ok := kms.GPUDevices[id]; !ok {
		kms.GPUDevices[id] = &GPUDevice{
			ID:     id,
			NodeID: nodeID,
		}

		kms.Metadata.ObjectCount++
	}

	return nil
}

func (kms *KubeModelSet) RegisterGPUUsage(id, containerID, gpuDeviceID string) error {
	if _, ok := kms.GPUUsages[id]; !ok {
		kms.GPUUsages[id] = &GPUUsage{
			ContainerID: containerID,
			GpuDeviceID: gpuDeviceID,
		}

		kms.Metadata.ObjectCount++
	}

	return nil
}

type kubeModelSetIndexes struct {
	namespaceNameToID map[string]string
}
