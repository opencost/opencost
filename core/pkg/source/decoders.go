package source

import (
	"github.com/opencost/opencost/core/pkg/util"
)

const (
	ClusterIDLabel       = "cluster_id"
	NamespaceLabel       = "namespace"
	NodeLabel            = "node"
	InstanceLabel        = "instance"
	InstanceTypeLabel    = "instance_type"
	ContainerLabel       = "container"
	PodLabel             = "pod"
	PodNameLabel         = "pod_name"
	ProviderIDLabel      = "provider_id"
	DeviceLabel          = "device"
	PVCLabel             = "persistentvolumeclaim"
	PVLabel              = "persistentvolume"
	StorageClassLabel    = "storageclass"
	VolumeNameLabel      = "volumename"
	ServiceLabel         = "service"
	ServiceNameLabel     = "service_name"
	IngressIPLabel       = "ingress_ip"
	ProvisionerNameLabel = "provisioner_name"
	UIDLabel             = "uid"
	KubernetesNodeLabel  = "kubernetes_node"
	ModeLabel            = "mode"
	ModelNameLabel       = "modelName"
	UUIDLabel            = "UUID"
	ResourceLabel        = "resource"
	DeploymentLabel      = "deployment"
	StatefulSetLabel     = "statefulSet"
	ReplicaSetLabel      = "replicaset"
	OwnerNameLabel       = "owner_name"
	OwnerKindLabel       = "owner_kind"
	UnitLabel            = "unit"
	InternetLabel        = "internet"
	SameZoneLabel        = "same_zone"
	SameRegionLabel      = "same_region"
)

const (
	NoneLabelValue = "<none>"
)

type PVResult struct {
	Cluster          string
	PersistentVolume string
}

type PVUsedAvgResult struct {
	Cluster               string
	Namespace             string
	PersistentVolumeClaim string

	Data []*util.Vector
}

func DecodePVUsedAvgResult(result *QueryResult) *PVUsedAvgResult {
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	pvc, _ := result.GetString(PVCLabel)

	return &PVUsedAvgResult{
		Cluster:               cluster,
		Namespace:             namespace,
		PersistentVolumeClaim: pvc,
		Data:                  result.Values,
	}
}

type PVActiveMinutesResult struct {
	Cluster          string
	PersistentVolume string

	Data []*util.Vector
}

func DecodePVActiveMinutesResult(result *QueryResult) *PVActiveMinutesResult {
	cluster, _ := result.GetCluster()
	pv, _ := result.GetString(PVLabel)

	return &PVActiveMinutesResult{
		Cluster:          cluster,
		PersistentVolume: pv,
		Data:             result.Values,
	}
}

type PVUsedMaxResult struct {
	Cluster               string
	Namespace             string
	PersistentVolumeClaim string
	Data                  []*util.Vector
}

func DecodePVUsedMaxResult(result *QueryResult) *PVUsedMaxResult {
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	pvc, _ := result.GetString(PVCLabel)

	return &PVUsedMaxResult{
		Cluster:               cluster,
		Namespace:             namespace,
		PersistentVolumeClaim: pvc,
		Data:                  result.Values,
	}
}

type LocalStorageActiveMinutesResult struct {
	Cluster    string
	Node       string
	ProviderID string

	Data []*util.Vector
}

func DecodeLocalStorageActiveMinutesResult(result *QueryResult) *LocalStorageActiveMinutesResult {
	cluster, _ := result.GetCluster()
	node, _ := result.GetNode()
	if node == "" {
		node, _ = result.GetInstance()
	}
	providerId, _ := result.GetProviderID()

	return &LocalStorageActiveMinutesResult{
		Cluster:    cluster,
		Node:       node,
		ProviderID: providerId,
		Data:       result.Values,
	}
}

type LocalStorageCostResult struct {
	Cluster  string
	Instance string
	Device   string

	Data []*util.Vector
}

func DecodeLocalStorageCostResult(result *QueryResult) *LocalStorageCostResult {
	cluster, _ := result.GetCluster()
	instance, _ := result.GetInstance()
	device, _ := result.GetDevice()

	return &LocalStorageCostResult{
		Cluster:  cluster,
		Instance: instance,
		Device:   device,
		Data:     result.Values,
	}
}

type LocalStorageUsedCostResult struct {
	Cluster  string
	Instance string
	Device   string
	Data     []*util.Vector
}

func DecodeLocalStorageUsedCostResult(result *QueryResult) *LocalStorageUsedCostResult {
	cluster, _ := result.GetCluster()
	instance, _ := result.GetInstance()
	device, _ := result.GetDevice()

	return &LocalStorageUsedCostResult{
		Cluster:  cluster,
		Instance: instance,
		Device:   device,
		Data:     result.Values,
	}
}

type LocalStorageUsedAvgResult struct {
	Cluster  string
	Instance string
	Device   string
	Data     []*util.Vector
}

func DecodeLocalStorageUsedAvgResult(result *QueryResult) *LocalStorageUsedAvgResult {
	cluster, _ := result.GetCluster()
	instance, _ := result.GetInstance()
	device, _ := result.GetDevice()

	return &LocalStorageUsedAvgResult{
		Cluster:  cluster,
		Instance: instance,
		Device:   device,
		Data:     result.Values,
	}
}

type LocalStorageUsedMaxResult struct {
	Cluster  string
	Instance string
	Device   string
	Data     []*util.Vector
}

func DecodeLocalStorageUsedMaxResult(result *QueryResult) *LocalStorageUsedMaxResult {
	cluster, _ := result.GetCluster()
	instance, _ := result.GetInstance()
	device, _ := result.GetDevice()

	return &LocalStorageUsedMaxResult{
		Cluster:  cluster,
		Instance: instance,
		Device:   device,
		Data:     result.Values,
	}
}

type LocalStorageBytesResult struct {
	Cluster  string
	Instance string
	Device   string
	Data     []*util.Vector
}

func DecodeLocalStorageBytesResult(result *QueryResult) *LocalStorageBytesResult {
	cluster, _ := result.GetCluster()
	instance, _ := result.GetInstance()
	device, _ := result.GetDevice()

	return &LocalStorageBytesResult{
		Cluster:  cluster,
		Instance: instance,
		Device:   device,
		Data:     result.Values,
	}
}

type NodeActiveMinutesResult struct {
	Cluster    string
	Node       string
	ProviderID string
	Data       []*util.Vector
}

func DecodeNodeActiveMinutesResult(result *QueryResult) *NodeActiveMinutesResult {
	cluster, _ := result.GetCluster()
	node, _ := result.GetNode()
	providerId, _ := result.GetProviderID()

	return &NodeActiveMinutesResult{
		Cluster:    cluster,
		Node:       node,
		ProviderID: providerId,
		Data:       result.Values,
	}
}

type NodeCPUCoresCapacityResult struct {
	Cluster string
	Node    string
	Data    []*util.Vector
}

func DecodeNodeCPUCoresCapacityResult(result *QueryResult) *NodeCPUCoresCapacityResult {
	cluster, _ := result.GetCluster()
	node, _ := result.GetNode()

	return &NodeCPUCoresCapacityResult{
		Cluster: cluster,
		Node:    node,
		Data:    result.Values,
	}
}

type NodeCPUCoresAllocatableResult = NodeCPUCoresCapacityResult

func DecodeNodeCPUCoresAllocatableResult(result *QueryResult) *NodeCPUCoresAllocatableResult {
	return DecodeNodeCPUCoresCapacityResult(result)
}

type NodeRAMBytesCapacityResult struct {
	Cluster string
	Node    string
	Data    []*util.Vector
}

func DecodeNodeRAMBytesCapacityResult(result *QueryResult) *NodeRAMBytesCapacityResult {
	cluster, _ := result.GetCluster()
	node, _ := result.GetNode()

	return &NodeRAMBytesCapacityResult{
		Cluster: cluster,
		Node:    node,
		Data:    result.Values,
	}
}

type NodeRAMBytesAllocatableResult = NodeRAMBytesCapacityResult

func DecodeNodeRAMBytesAllocatableResult(result *QueryResult) *NodeRAMBytesAllocatableResult {
	return DecodeNodeRAMBytesCapacityResult(result)
}

type NodeGPUCountResult struct {
	Cluster    string
	Node       string
	ProviderID string

	Data []*util.Vector
}

func DecodeNodeGPUCountResult(result *QueryResult) *NodeGPUCountResult {
	cluster, _ := result.GetCluster()
	node, _ := result.GetNode()
	providerId, _ := result.GetProviderID()

	return &NodeGPUCountResult{
		Cluster:    cluster,
		Node:       node,
		ProviderID: providerId,
		Data:       result.Values,
	}
}

type NodeCPUModeTotalResult struct {
	Cluster string
	Node    string
	Mode    string
	Data    []*util.Vector
}

func DecodeNodeCPUModeTotalResult(result *QueryResult) *NodeCPUModeTotalResult {
	cluster, _ := result.GetCluster()
	node, _ := result.GetString(KubernetesNodeLabel)
	mode, _ := result.GetString(ModeLabel)

	return &NodeCPUModeTotalResult{
		Cluster: cluster,
		Node:    node,
		Mode:    mode,
		Data:    result.Values,
	}
}

type NodeIsSpotResult struct {
	Cluster    string
	Node       string
	ProviderID string
	Data       []*util.Vector
}

func DecodeNodeIsSpotResult(result *QueryResult) *NodeIsSpotResult {
	cluster, _ := result.GetCluster()
	node, _ := result.GetNode()
	providerId, _ := result.GetProviderID()

	return &NodeIsSpotResult{
		Cluster:    cluster,
		Node:       node,
		ProviderID: providerId,
		Data:       result.Values,
	}
}

type NodeRAMSystemPercentResult struct {
	Cluster  string
	Instance string
	Data     []*util.Vector
}

func DecodeNodeRAMSystemPercentResult(result *QueryResult) *NodeRAMSystemPercentResult {
	cluster, _ := result.GetCluster()
	instance, _ := result.GetInstance()

	return &NodeRAMSystemPercentResult{
		Cluster:  cluster,
		Instance: instance,
		Data:     result.Values,
	}
}

type NodeRAMUserPercentResult = NodeRAMSystemPercentResult

func DecodeNodeRAMUserPercentResult(result *QueryResult) *NodeRAMUserPercentResult {
	return DecodeNodeRAMSystemPercentResult(result)
}

type LBActiveMinutesResult struct {
	Cluster   string
	Namespace string
	Service   string
	IngressIP string

	Data []*util.Vector
}

func DecodeLBActiveMinutesResult(result *QueryResult) *LBActiveMinutesResult {
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	service, _ := result.GetString(ServiceNameLabel)
	ingressIp, _ := result.GetString(IngressIPLabel)

	return &LBActiveMinutesResult{
		Cluster:   cluster,
		Namespace: namespace,
		Service:   service,
		IngressIP: ingressIp,
		Data:      result.Values,
	}
}

type LBPricePerHrResult = LBActiveMinutesResult

func DecodeLBPricePerHrResult(result *QueryResult) *LBPricePerHrResult {
	return DecodeLBActiveMinutesResult(result)
}

type ClusterManagementDurationResult struct {
	Cluster     string
	Provisioner string
	Data        []*util.Vector
}

func DecodeClusterManagementDurationResult(result *QueryResult) *ClusterManagementDurationResult {
	cluster, _ := result.GetCluster()
	provisioner, _ := result.GetString(ProvisionerNameLabel)

	return &ClusterManagementDurationResult{
		Cluster:     cluster,
		Provisioner: provisioner,
		Data:        result.Values,
	}
}

type ClusterManagementPricePerHrResult = ClusterManagementDurationResult

func DecodeClusterManagementPricePerHrResult(result *QueryResult) *ClusterManagementPricePerHrResult {
	return DecodeClusterManagementDurationResult(result)
}

type PodsResult struct {
	UID       string
	Cluster   string
	Namespace string
	Pod       string

	Data []*util.Vector
}

func DecodePodsResult(result *QueryResult) *PodsResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	pod, _ := result.GetPod()

	return &PodsResult{
		UID:       uid,
		Cluster:   cluster,
		Namespace: namespace,
		Pod:       pod,
		Data:      result.Values,
	}
}

type ContainerMetricResult struct {
	Cluster   string
	Node      string
	Instance  string
	Namespace string
	Pod       string
	Container string

	Data []*util.Vector
}

func DecodeContainerMetricResult(result *QueryResult) *ContainerMetricResult {
	cluster, _ := result.GetCluster()

	node, _ := result.GetNode()
	instance, _ := result.GetInstance()

	// NOTE: this addresses cases where the node isn't set, but the instance is,
	// NOTE: we just inherit the instance as the node
	if node == "" {
		node = instance
	}

	namespace, _ := result.GetNamespace()
	pod, _ := result.GetPod()
	container, _ := result.GetContainer()

	return &ContainerMetricResult{
		Cluster:   cluster,
		Node:      node,
		Instance:  instance,
		Namespace: namespace,
		Pod:       pod,
		Container: container,
		Data:      result.Values,
	}
}

type RAMBytesAllocatedResult = ContainerMetricResult

func DecodeRAMBytesAllocatedResult(result *QueryResult) *RAMBytesAllocatedResult {
	return DecodeContainerMetricResult(result)
}

type RAMRequestsResult = ContainerMetricResult

func DecodeRAMRequestsResult(result *QueryResult) *RAMRequestsResult {
	return DecodeContainerMetricResult(result)
}

type RAMUsageAvgResult = ContainerMetricResult

func DecodeRAMUsageAvgResult(result *QueryResult) *RAMUsageAvgResult {
	return DecodeContainerMetricResult(result)
}

type RAMUsageMaxResult = ContainerMetricResult

func DecodeRAMUsageMaxResult(result *QueryResult) *RAMUsageMaxResult {
	return DecodeContainerMetricResult(result)
}

type NodeRAMPricePerGiBHrResult struct {
	Cluster      string
	Node         string
	InstanceType string
	ProviderID   string
	Data         []*util.Vector
}

func DecodeNodeRAMPricePerGiBHrResult(result *QueryResult) *NodeRAMPricePerGiBHrResult {
	cluster, _ := result.GetCluster()
	node, _ := result.GetNode()
	instanceType, _ := result.GetInstanceType()
	providerId, _ := result.GetProviderID()

	return &NodeRAMPricePerGiBHrResult{
		Cluster:      cluster,
		Node:         node,
		InstanceType: instanceType,
		ProviderID:   providerId,
		Data:         result.Values,
	}
}

type CPUCoresAllocatedResult = ContainerMetricResult

func DecodeCPUCoresAllocatedResult(result *QueryResult) *CPUCoresAllocatedResult {
	return DecodeContainerMetricResult(result)
}

type CPURequestsResult = ContainerMetricResult

func DecodeCPURequestsResult(result *QueryResult) *CPURequestsResult {
	return DecodeContainerMetricResult(result)
}

type CPUUsageAvgResult = ContainerMetricResult

func DecodeCPUUsageAvgResult(result *QueryResult) *CPUUsageAvgResult {
	return DecodeContainerMetricResult(result)
}

type CPUUsageMaxResult = ContainerMetricResult

func DecodeCPUUsageMaxResult(result *QueryResult) *CPUUsageMaxResult {
	return DecodeContainerMetricResult(result)
}

type NodeCPUPricePerHrResult struct {
	Cluster      string
	Node         string
	InstanceType string
	ProviderID   string
	Data         []*util.Vector
}

func DecodeNodeCPUPricePerHrResult(result *QueryResult) *NodeCPUPricePerHrResult {
	cluster, _ := result.GetCluster()
	node, _ := result.GetNode()
	instanceType, _ := result.GetInstanceType()
	providerId, _ := result.GetProviderID()

	return &NodeCPUPricePerHrResult{
		Cluster:      cluster,
		Node:         node,
		InstanceType: instanceType,
		ProviderID:   providerId,
		Data:         result.Values,
	}
}

// type alias requested result to allocated result, as you can only request a full GPU
type GPUsRequestedResult = GPUsAllocatedResult

func DecodeGPUsRequestedResult(result *QueryResult) *GPUsRequestedResult {
	return DecodeGPUsAllocatedResult(result)
}

type GPUsAllocatedResult struct {
	Cluster   string
	Namespace string
	Pod       string
	Container string
	Data      []*util.Vector
}

func DecodeGPUsAllocatedResult(result *QueryResult) *GPUsAllocatedResult {
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	pod, _ := result.GetPod()
	container, _ := result.GetContainer()

	return &GPUsAllocatedResult{
		Cluster:   cluster,
		Namespace: namespace,
		Pod:       pod,
		Container: container,
		Data:      result.Values,
	}
}

type GPUsUsageAvgResult struct {
	Cluster   string
	Namespace string
	Pod       string
	Container string

	Data []*util.Vector
}

func DecodeGPUsUsageAvgResult(result *QueryResult) *GPUsUsageAvgResult {
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	pod, _ := result.GetPod()
	container, _ := result.GetContainer()

	return &GPUsUsageAvgResult{
		Cluster:   cluster,
		Namespace: namespace,
		Pod:       pod,
		Container: container,
		Data:      result.Values,
	}
}

type GPUsUsageMaxResult struct {
	Cluster   string
	Namespace string
	Pod       string
	Container string
	Data      []*util.Vector
}

func DecodeGPUsUsageMaxResult(result *QueryResult) *GPUsUsageMaxResult {
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	pod, _ := result.GetPod()
	container, _ := result.GetContainer()

	return &GPUsUsageMaxResult{
		Cluster:   cluster,
		Namespace: namespace,
		Pod:       pod,
		Container: container,
		Data:      result.Values,
	}
}

type NodeGPUPricePerHrResult struct {
	Cluster      string
	Node         string
	InstanceType string
	ProviderID   string
	Data         []*util.Vector
}

func DecodeNodeGPUPricePerHrResult(result *QueryResult) *NodeGPUPricePerHrResult {
	cluster, _ := result.GetCluster()
	node, _ := result.GetNode()
	instanceType, _ := result.GetInstanceType()
	providerId, _ := result.GetProviderID()

	return &NodeGPUPricePerHrResult{
		Cluster:      cluster,
		Node:         node,
		InstanceType: instanceType,
		ProviderID:   providerId,
		Data:         result.Values,
	}
}

type GPUInfoResult struct {
	Cluster   string
	Namespace string
	Pod       string
	Container string
	Device    string
	ModelName string
	UUID      string
	Data      []*util.Vector
}

func DecodeGPUInfoResult(result *QueryResult) *GPUInfoResult {
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	pod, _ := result.GetPod()
	container, _ := result.GetContainer()
	device, _ := result.GetString(DeviceLabel)
	modelName, _ := result.GetString(ModelNameLabel)
	uuid, _ := result.GetString(UUIDLabel)

	return &GPUInfoResult{
		Cluster:   cluster,
		Namespace: namespace,
		Pod:       pod,
		Container: container,
		Device:    device,
		ModelName: modelName,
		UUID:      uuid,
		Data:      result.Values,
	}
}

type IsGPUSharedResult struct {
	Cluster   string
	Namespace string
	Pod       string
	Container string
	Resource  string
	Data      []*util.Vector
}

func DecodeIsGPUSharedResult(result *QueryResult) *IsGPUSharedResult {
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	pod, _ := result.GetPod()
	container, _ := result.GetContainer()
	resource, _ := result.GetString(ResourceLabel)

	return &IsGPUSharedResult{
		Cluster:   cluster,
		Namespace: namespace,
		Pod:       pod,
		Container: container,
		Resource:  resource,
		Data:      result.Values,
	}
}

type PodPVCAllocationResult struct {
	Cluster               string
	Namespace             string
	Pod                   string
	PersistentVolume      string
	PersistentVolumeClaim string
	Data                  []*util.Vector
}

func DecodePodPVCAllocationResult(result *QueryResult) *PodPVCAllocationResult {
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	pod, _ := result.GetPod()
	pv, _ := result.GetString(PVLabel)
	pvc, _ := result.GetString(PVCLabel)

	return &PodPVCAllocationResult{
		Cluster:               cluster,
		Namespace:             namespace,
		Pod:                   pod,
		PersistentVolume:      pv,
		PersistentVolumeClaim: pvc,
		Data:                  result.Values,
	}
}

type PVCBytesRequestedResult struct {
	Cluster               string
	Namespace             string
	PersistentVolumeClaim string

	Data []*util.Vector
}

func DecodePVCBytesRequestedResult(result *QueryResult) *PVCBytesRequestedResult {
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	pvc, _ := result.GetString(PVCLabel)

	return &PVCBytesRequestedResult{
		Cluster:               cluster,
		Namespace:             namespace,
		PersistentVolumeClaim: pvc,
		Data:                  result.Values,
	}
}

type PVCInfoResult struct {
	Cluster               string
	Namespace             string
	VolumeName            string
	PersistentVolumeClaim string
	StorageClass          string

	Data []*util.Vector
}

func DecodePVCInfoResult(result *QueryResult) *PVCInfoResult {
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	volumeName, _ := result.GetString(VolumeNameLabel)
	pvc, _ := result.GetString(PVCLabel)
	storageClass, _ := result.GetString(StorageClassLabel)

	return &PVCInfoResult{
		Cluster:               cluster,
		Namespace:             namespace,
		VolumeName:            volumeName,
		PersistentVolumeClaim: pvc,
		StorageClass:          storageClass,
		Data:                  result.Values,
	}
}

type PVBytesResult struct {
	Cluster          string
	PersistentVolume string

	Data []*util.Vector
}

func DecodePVBytesResult(result *QueryResult) *PVBytesResult {
	cluster, _ := result.GetCluster()
	pv, _ := result.GetString(PVLabel)

	return &PVBytesResult{
		Cluster:          cluster,
		PersistentVolume: pv,
		Data:             result.Values,
	}
}

type PVPricePerGiBHourResult struct {
	Cluster          string
	VolumeName       string
	PersistentVolume string
	ProviderID       string

	Data []*util.Vector
}

func DecodePVPricePerGiBHourResult(result *QueryResult) *PVPricePerGiBHourResult {
	cluster, _ := result.GetCluster()
	volumeName, _ := result.GetString(VolumeNameLabel)
	pv, _ := result.GetString(PVLabel)
	providerId, _ := result.GetProviderID()

	return &PVPricePerGiBHourResult{
		Cluster:          cluster,
		VolumeName:       volumeName,
		PersistentVolume: pv,
		ProviderID:       providerId,

		Data: result.Values,
	}
}

type PVInfoResult struct {
	Cluster          string
	PersistentVolume string
	StorageClass     string
	ProviderID       string

	Data []*util.Vector
}

func DecodePVInfoResult(result *QueryResult) *PVInfoResult {
	cluster, _ := result.GetCluster()
	storageClass, _ := result.GetString(StorageClassLabel)
	providerId, _ := result.GetProviderID()
	pv, _ := result.GetString(PVLabel)

	return &PVInfoResult{
		Cluster:          cluster,
		PersistentVolume: pv,
		StorageClass:     storageClass,
		ProviderID:       providerId,
		Data:             result.Values,
	}
}

// Base type for network usage results
type NetworkGiBResult struct {
	Cluster   string
	Namespace string
	Pod       string
	Service   string

	Data []*util.Vector
}

func DecodeNetworkGiBResult(result *QueryResult) *NetworkGiBResult {
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	pod, _ := result.GetPod()
	service, _ := result.GetString(ServiceLabel)

	return &NetworkGiBResult{
		Cluster:   cluster,
		Namespace: namespace,
		Pod:       pod,
		Service:   service,
		Data:      result.Values,
	}
}

// Base type for network price results
type NetworkPricePerGiBResult struct {
	Cluster string

	Data []*util.Vector
}

func DecodeNetworkPricePerGiBResult(result *QueryResult) *NetworkPricePerGiBResult {
	cluster, _ := result.GetCluster()

	return &NetworkPricePerGiBResult{
		Cluster: cluster,
		Data:    result.Values,
	}
}

// Type alias the specific network subclassification results AND price results
type NetZoneGiBResult = NetworkGiBResult
type NetZonePricePerGiBResult = NetworkPricePerGiBResult

type NetRegionGiBResult = NetworkGiBResult
type NetRegionPricePerGiBResult = NetworkPricePerGiBResult

type NetInternetGiBResult = NetworkGiBResult
type NetInternetPricePerGiBResult = NetworkPricePerGiBResult

type NetInternetServiceGiBResult = NetworkGiBResult

type NetZoneIngressGiBResult = NetworkGiBResult
type NetRegionIngressGiBResult = NetworkGiBResult
type NetInternetIngressGiBResult = NetworkGiBResult
type NetInternetServiceIngressGiBResult = NetworkGiBResult

func DecodeNetZoneGiBResult(result *QueryResult) *NetZoneGiBResult {
	return DecodeNetworkGiBResult(result)
}

func DecodeNetZonePricePerGiBResult(result *QueryResult) *NetZonePricePerGiBResult {
	return DecodeNetworkPricePerGiBResult(result)
}

func DecodeNetRegionGiBResult(result *QueryResult) *NetRegionGiBResult {
	return DecodeNetworkGiBResult(result)
}

func DecodeNetRegionPricePerGiBResult(result *QueryResult) *NetRegionPricePerGiBResult {
	return DecodeNetworkPricePerGiBResult(result)
}

func DecodeNetInternetGiBResult(result *QueryResult) *NetInternetGiBResult {
	return DecodeNetworkGiBResult(result)
}

func DecodeNetInternetPricePerGiBResult(result *QueryResult) *NetInternetPricePerGiBResult {
	return DecodeNetworkPricePerGiBResult(result)
}

func DecodeNetInternetServiceGiBResult(result *QueryResult) *NetInternetServiceGiBResult {
	return DecodeNetworkGiBResult(result)
}

func DecodeNetZoneIngressGiBResult(result *QueryResult) *NetZoneIngressGiBResult {
	return DecodeNetworkGiBResult(result)
}

func DecodeNetRegionIngressGiBResult(result *QueryResult) *NetRegionIngressGiBResult {
	return DecodeNetworkGiBResult(result)
}

func DecodeNetInternetIngressGiBResult(result *QueryResult) *NetInternetIngressGiBResult {
	return DecodeNetworkGiBResult(result)
}

func DecodeNetInternetServiceIngressGiBResult(result *QueryResult) *NetInternetServiceIngressGiBResult {
	return DecodeNetworkGiBResult(result)
}

type NetReceiveBytesResult struct {
	Cluster   string
	Namespace string
	Pod       string
	Container string
	Data      []*util.Vector
}

func DecodeNetReceiveBytesResult(result *QueryResult) *NetReceiveBytesResult {
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	pod, _ := result.GetPod()
	container, _ := result.GetContainer()

	return &NetReceiveBytesResult{
		Cluster:   cluster,
		Namespace: namespace,
		Pod:       pod,
		Container: container,
		Data:      result.Values,
	}
}

type NetTransferBytesResult struct {
	Cluster   string
	Namespace string
	Pod       string
	Container string

	Data []*util.Vector
}

func DecodeNetTransferBytesResult(result *QueryResult) *NetTransferBytesResult {
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	pod, _ := result.GetPod()
	container, _ := result.GetContainer()

	return &NetTransferBytesResult{
		Cluster:   cluster,
		Namespace: namespace,
		Pod:       pod,
		Container: container,
		Data:      result.Values,
	}
}

type NamespaceAnnotationsResult struct {
	Cluster     string
	Namespace   string
	Annotations map[string]string

	Data []*util.Vector
}

func DecodeNamespaceAnnotationsResult(result *QueryResult) *NamespaceAnnotationsResult {
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	annotations := result.GetAnnotations()

	return &NamespaceAnnotationsResult{
		Cluster:     cluster,
		Namespace:   namespace,
		Annotations: annotations,
		Data:        result.Values,
	}
}

type PodAnnotationsResult struct {
	Cluster     string
	Namespace   string
	Pod         string
	Annotations map[string]string

	Data []*util.Vector
}

func DecodePodAnnotationsResult(result *QueryResult) *PodAnnotationsResult {
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	pod, _ := result.GetPod()
	annotations := result.GetAnnotations()

	return &PodAnnotationsResult{
		Cluster:     cluster,
		Namespace:   namespace,
		Pod:         pod,
		Annotations: annotations,
		Data:        result.Values,
	}
}

type NodeLabelsResult struct {
	Cluster string
	Node    string
	Labels  map[string]string
	Data    []*util.Vector
}

func DecodeNodeLabelsResult(result *QueryResult) *NodeLabelsResult {
	cluster, _ := result.GetCluster()
	node, _ := result.GetNode()
	labels := result.GetLabels()

	return &NodeLabelsResult{
		Cluster: cluster,
		Node:    node,
		Labels:  labels,
		Data:    result.Values,
	}
}

type NamespaceLabelsResult struct {
	Cluster   string
	Namespace string
	Labels    map[string]string
	Data      []*util.Vector
}

func DecodeNamespaceLabelsResult(result *QueryResult) *NamespaceLabelsResult {
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	labels := result.GetLabels()

	return &NamespaceLabelsResult{
		Cluster:   cluster,
		Namespace: namespace,
		Labels:    labels,
		Data:      result.Values,
	}
}

type PodLabelsResult struct {
	Cluster   string
	Namespace string
	Pod       string
	Labels    map[string]string
	Data      []*util.Vector
}

func DecodePodLabelsResult(result *QueryResult) *PodLabelsResult {
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	pod, _ := result.GetPod()
	labels := result.GetLabels()

	return &PodLabelsResult{
		Cluster:   cluster,
		Namespace: namespace,
		Pod:       pod,
		Labels:    labels,
		Data:      result.Values,
	}
}

type ServiceLabelsResult struct {
	Cluster   string
	Namespace string
	Service   string
	Labels    map[string]string

	Data []*util.Vector
}

func DecodeServiceLabelsResult(result *QueryResult) *ServiceLabelsResult {
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	service, _ := result.GetString(ServiceLabel)
	labels := result.GetLabels()

	return &ServiceLabelsResult{
		Cluster:   cluster,
		Namespace: namespace,
		Service:   service,
		Labels:    labels,
		Data:      result.Values,
	}
}

type DeploymentLabelsResult struct {
	Cluster    string
	Namespace  string
	Deployment string
	Labels     map[string]string
	Data       []*util.Vector
}

func DecodeDeploymentLabelsResult(result *QueryResult) *DeploymentLabelsResult {
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	deployment, _ := result.GetString(DeploymentLabel)
	labels := result.GetLabels()

	return &DeploymentLabelsResult{
		Cluster:    cluster,
		Namespace:  namespace,
		Deployment: deployment,
		Labels:     labels,
		Data:       result.Values,
	}
}

type StatefulSetLabelsResult struct {
	Cluster     string
	Namespace   string
	StatefulSet string
	Labels      map[string]string
	Data        []*util.Vector
}

func DecodeStatefulSetLabelsResult(result *QueryResult) *StatefulSetLabelsResult {
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	statefulSet, _ := result.GetString(StatefulSetLabel)
	labels := result.GetLabels()

	return &StatefulSetLabelsResult{
		Cluster:     cluster,
		Namespace:   namespace,
		StatefulSet: statefulSet,
		Labels:      labels,
		Data:        result.Values,
	}
}

type DaemonSetLabelsResult struct {
	Cluster   string
	Namespace string
	Pod       string
	DaemonSet string
	Labels    map[string]string
	Data      []*util.Vector
}

func DecodeDaemonSetLabelsResult(result *QueryResult) *DaemonSetLabelsResult {
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	pod, _ := result.GetPod()
	daemonSet, _ := result.GetString(OwnerNameLabel)
	labels := result.GetLabels()

	return &DaemonSetLabelsResult{
		Cluster:   cluster,
		Namespace: namespace,
		Pod:       pod,
		DaemonSet: daemonSet,
		Labels:    labels,
		Data:      result.Values,
	}
}

type JobLabelsResult struct {
	Cluster   string
	Namespace string
	Pod       string
	Job       string
	Labels    map[string]string
	Data      []*util.Vector
}

func DecodeJobLabelsResult(result *QueryResult) *JobLabelsResult {
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	pod, _ := result.GetPod()
	job, _ := result.GetString(OwnerNameLabel)
	labels := result.GetLabels()

	return &JobLabelsResult{
		Cluster:   cluster,
		Namespace: namespace,
		Pod:       pod,
		Job:       job,
		Labels:    labels,
		Data:      result.Values,
	}
}

type PodsWithReplicaSetOwnerResult struct {
	Cluster    string
	Namespace  string
	Pod        string
	ReplicaSet string

	Data []*util.Vector
}

func DecodePodsWithReplicaSetOwnerResult(result *QueryResult) *PodsWithReplicaSetOwnerResult {
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	replicaSet, _ := result.GetString(OwnerNameLabel)
	pod, _ := result.GetPod()

	return &PodsWithReplicaSetOwnerResult{
		Cluster:    cluster,
		Namespace:  namespace,
		Pod:        pod,
		ReplicaSet: replicaSet,
		Data:       result.Values,
	}
}

type ReplicaSetsWithoutOwnersResult struct {
	Cluster    string
	Namespace  string
	ReplicaSet string

	Data []*util.Vector
}

func DecodeReplicaSetsWithoutOwnersResult(result *QueryResult) *ReplicaSetsWithoutOwnersResult {
	return &ReplicaSetsWithoutOwnersResult{
		Data: result.Values,
	}
}

type ReplicaSetsWithRolloutResult struct {
	Cluster    string
	Namespace  string
	ReplicaSet string
	OwnerName  string
	OwnerKind  string
	Data       []*util.Vector
}

func DecodeReplicaSetsWithRolloutResult(result *QueryResult) *ReplicaSetsWithRolloutResult {
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	replicaSet, _ := result.GetString(ReplicaSetLabel)
	ownerName, _ := result.GetString(OwnerNameLabel)
	ownerKind, _ := result.GetString(OwnerKindLabel)

	return &ReplicaSetsWithRolloutResult{
		Cluster:    cluster,
		Namespace:  namespace,
		ReplicaSet: replicaSet,
		OwnerName:  ownerName,
		OwnerKind:  ownerKind,
		Data:       result.Values,
	}
}

func DecodeAll[T any](results []*QueryResult, decode ResultDecoder[T]) []*T {
	decoded := make([]*T, 0, len(results))
	for _, result := range results {
		decoded = append(decoded, decode(result))
	}

	return decoded
}
