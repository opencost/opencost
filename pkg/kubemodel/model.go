package kubemodel

import (
	"github.com/opencost/opencost/core/pkg/model/pb"
	kubepb "github.com/opencost/opencost/core/pkg/model/pb/kubemodel"
	"google.golang.org/protobuf/proto"
)

// Model groups every resource snapshot that composes a Kubecost cluster model.
// Each map is keyed by the resource's stable Kubernetes UID (or equivalent)
// to make deduplication and lookups straightforward.
type Model struct {
	Window  *pb.Window
	Cluster *kubepb.Cluster

	Nodes       map[string]*kubepb.Node
	GPUDevices  map[string]*kubepb.GPUDevice
	Volumes     map[string]*kubepb.Volume
	PVCs        map[string]*kubepb.PersistentVolumeClaim
	Controllers map[string]*kubepb.Controller
	Namespaces  map[string]*kubepb.Namespace
	Pods        map[string]*kubepb.Pod
	Containers  map[string]*kubepb.Container
	Services    map[string]*kubepb.Service
}

// NewModel returns an empty Model with all maps initialised.
func NewModel() *Model {
	return &Model{
		Nodes:       make(map[string]*kubepb.Node),
		GPUDevices:  make(map[string]*kubepb.GPUDevice),
		Volumes:     make(map[string]*kubepb.Volume),
		PVCs:        make(map[string]*kubepb.PersistentVolumeClaim),
		Controllers: make(map[string]*kubepb.Controller),
		Namespaces:  make(map[string]*kubepb.Namespace),
		Pods:        make(map[string]*kubepb.Pod),
		Containers:  make(map[string]*kubepb.Container),
		Services:    make(map[string]*kubepb.Service),
	}
}

// Clone produces a deep copy of the Model so the caller can mutate it safely.
func (m *Model) Clone() *Model {
	if m == nil {
		return nil
	}

	cloned := NewModel()
	if m.Window != nil {
		cloned.Window = proto.Clone(m.Window).(*pb.Window)
	}
	if m.Cluster != nil {
		cloned.Cluster = proto.Clone(m.Cluster).(*kubepb.Cluster)
	}

	for id, node := range m.Nodes {
		cloned.Nodes[id] = proto.Clone(node).(*kubepb.Node)
	}
	for id, gpu := range m.GPUDevices {
		cloned.GPUDevices[id] = proto.Clone(gpu).(*kubepb.GPUDevice)
	}
	for id, volume := range m.Volumes {
		cloned.Volumes[id] = proto.Clone(volume).(*kubepb.Volume)
	}
	for id, pvc := range m.PVCs {
		cloned.PVCs[id] = proto.Clone(pvc).(*kubepb.PersistentVolumeClaim)
	}
	for id, controller := range m.Controllers {
		cloned.Controllers[id] = proto.Clone(controller).(*kubepb.Controller)
	}
	for id, ns := range m.Namespaces {
		cloned.Namespaces[id] = proto.Clone(ns).(*kubepb.Namespace)
	}
	for id, pod := range m.Pods {
		cloned.Pods[id] = proto.Clone(pod).(*kubepb.Pod)
	}
	for id, container := range m.Containers {
		cloned.Containers[id] = proto.Clone(container).(*kubepb.Container)
	}
	for id, svc := range m.Services {
		cloned.Services[id] = proto.Clone(svc).(*kubepb.Service)
	}

	return cloned
}

// Merge copies every resource from the input model into the receiver.
// Existing entries with the same identifier are overwritten.
func (m *Model) Merge(other *Model) {
	if m == nil || other == nil {
		return
	}

	if other.Window != nil {
		m.Window = proto.Clone(other.Window).(*pb.Window)
	}
	if other.Cluster != nil {
		m.Cluster = proto.Clone(other.Cluster).(*kubepb.Cluster)
	}

	for id, node := range other.Nodes {
		m.Nodes[id] = proto.Clone(node).(*kubepb.Node)
	}
	for id, gpu := range other.GPUDevices {
		m.GPUDevices[id] = proto.Clone(gpu).(*kubepb.GPUDevice)
	}
	for id, volume := range other.Volumes {
		m.Volumes[id] = proto.Clone(volume).(*kubepb.Volume)
	}
	for id, pvc := range other.PVCs {
		m.PVCs[id] = proto.Clone(pvc).(*kubepb.PersistentVolumeClaim)
	}
	for id, controller := range other.Controllers {
		m.Controllers[id] = proto.Clone(controller).(*kubepb.Controller)
	}
	for id, ns := range other.Namespaces {
		m.Namespaces[id] = proto.Clone(ns).(*kubepb.Namespace)
	}
	for id, pod := range other.Pods {
		m.Pods[id] = proto.Clone(pod).(*kubepb.Pod)
	}
	for id, container := range other.Containers {
		m.Containers[id] = proto.Clone(container).(*kubepb.Container)
	}
	for id, svc := range other.Services {
		m.Services[id] = proto.Clone(svc).(*kubepb.Service)
	}
}
