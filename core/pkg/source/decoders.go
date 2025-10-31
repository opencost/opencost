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
	ResourceQuotaLabel   = "resourcequota"
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
	UID              string
	Cluster          string
	PersistentVolume string
}

type PVUsedAvgResult struct {
	UID                   string
	Cluster               string
	Namespace             string
	PersistentVolumeClaim string

	Data []*util.Vector
}

func DecodePVUsedAvgResult(result *QueryResult) *PVUsedAvgResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	pvc, _ := result.GetString(PVCLabel)

	return &PVUsedAvgResult{
		UID:                   uid,
		Cluster:               cluster,
		Namespace:             namespace,
		PersistentVolumeClaim: pvc,
		Data:                  result.Values,
	}
}

type PVActiveMinutesResult struct {
	UID              string
	Cluster          string
	PersistentVolume string

	Data []*util.Vector
}

func DecodePVActiveMinutesResult(result *QueryResult) *PVActiveMinutesResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	pv, _ := result.GetString(PVLabel)

	return &PVActiveMinutesResult{
		UID:              uid,
		Cluster:          cluster,
		PersistentVolume: pv,
		Data:             result.Values,
	}
}

type PVUsedMaxResult struct {
	UID                   string
	Cluster               string
	Namespace             string
	PersistentVolumeClaim string
	Data                  []*util.Vector
}

func DecodePVUsedMaxResult(result *QueryResult) *PVUsedMaxResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	pvc, _ := result.GetString(PVCLabel)

	return &PVUsedMaxResult{
		UID:                   uid,
		Cluster:               cluster,
		Namespace:             namespace,
		PersistentVolumeClaim: pvc,
		Data:                  result.Values,
	}
}

type LocalStorageActiveMinutesResult struct {
	UID        string
	Cluster    string
	Node       string
	ProviderID string

	Data []*util.Vector
}

func DecodeLocalStorageActiveMinutesResult(result *QueryResult) *LocalStorageActiveMinutesResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	node, _ := result.GetNode()
	if node == "" {
		node, _ = result.GetInstance()
	}
	providerId, _ := result.GetProviderID()

	return &LocalStorageActiveMinutesResult{
		UID:        uid,
		Cluster:    cluster,
		Node:       node,
		ProviderID: providerId,
		Data:       result.Values,
	}
}

type LocalStorageCostResult struct {
	UID      string
	Cluster  string
	Instance string
	Device   string

	Data []*util.Vector
}

func DecodeLocalStorageCostResult(result *QueryResult) *LocalStorageCostResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	instance, _ := result.GetInstance()
	device, _ := result.GetDevice()

	return &LocalStorageCostResult{
		UID:      uid,
		Cluster:  cluster,
		Instance: instance,
		Device:   device,
		Data:     result.Values,
	}
}

type LocalStorageUsedCostResult struct {
	UID      string
	Cluster  string
	Instance string
	Device   string
	Data     []*util.Vector
}

func DecodeLocalStorageUsedCostResult(result *QueryResult) *LocalStorageUsedCostResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	instance, _ := result.GetInstance()
	device, _ := result.GetDevice()

	return &LocalStorageUsedCostResult{
		UID:      uid,
		Cluster:  cluster,
		Instance: instance,
		Device:   device,
		Data:     result.Values,
	}
}

type LocalStorageUsedAvgResult struct {
	UID      string
	Cluster  string
	Instance string
	Device   string
	Data     []*util.Vector
}

func DecodeLocalStorageUsedAvgResult(result *QueryResult) *LocalStorageUsedAvgResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	instance, _ := result.GetInstance()
	device, _ := result.GetDevice()

	return &LocalStorageUsedAvgResult{
		UID:      uid,
		Cluster:  cluster,
		Instance: instance,
		Device:   device,
		Data:     result.Values,
	}
}

type LocalStorageUsedMaxResult struct {
	UID      string
	Cluster  string
	Instance string
	Device   string
	Data     []*util.Vector
}

func DecodeLocalStorageUsedMaxResult(result *QueryResult) *LocalStorageUsedMaxResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	instance, _ := result.GetInstance()
	device, _ := result.GetDevice()

	return &LocalStorageUsedMaxResult{
		UID:      uid,
		Cluster:  cluster,
		Instance: instance,
		Device:   device,
		Data:     result.Values,
	}
}

type LocalStorageBytesResult struct {
	UID      string
	Cluster  string
	Instance string
	Device   string
	Data     []*util.Vector
}

func DecodeLocalStorageBytesResult(result *QueryResult) *LocalStorageBytesResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	instance, _ := result.GetInstance()
	device, _ := result.GetDevice()

	return &LocalStorageBytesResult{
		UID:      uid,
		Cluster:  cluster,
		Instance: instance,
		Device:   device,
		Data:     result.Values,
	}
}

type NodeActiveMinutesResult struct {
	UID        string
	Cluster    string
	Node       string
	ProviderID string
	Data       []*util.Vector
}

func DecodeNodeActiveMinutesResult(result *QueryResult) *NodeActiveMinutesResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	node, _ := result.GetNode()
	providerId, _ := result.GetProviderID()

	return &NodeActiveMinutesResult{
		UID:        uid,
		Cluster:    cluster,
		Node:       node,
		ProviderID: providerId,
		Data:       result.Values,
	}
}

type NodeCPUCoresCapacityResult struct {
	UID     string
	Cluster string
	Node    string
	Data    []*util.Vector
}

func DecodeNodeCPUCoresCapacityResult(result *QueryResult) *NodeCPUCoresCapacityResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	node, _ := result.GetNode()

	return &NodeCPUCoresCapacityResult{
		UID:     uid,
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
	UID     string
	Cluster string
	Node    string
	Data    []*util.Vector
}

func DecodeNodeRAMBytesCapacityResult(result *QueryResult) *NodeRAMBytesCapacityResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	node, _ := result.GetNode()

	return &NodeRAMBytesCapacityResult{
		UID:     uid,
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
	UID        string
	Cluster    string
	Node       string
	ProviderID string

	Data []*util.Vector
}

func DecodeNodeGPUCountResult(result *QueryResult) *NodeGPUCountResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	node, _ := result.GetNode()
	providerId, _ := result.GetProviderID()

	return &NodeGPUCountResult{
		UID:        uid,
		Cluster:    cluster,
		Node:       node,
		ProviderID: providerId,
		Data:       result.Values,
	}
}

type NodeCPUModeTotalResult struct {
	UID     string
	Cluster string
	Node    string
	Mode    string
	Data    []*util.Vector
}

func DecodeNodeCPUModeTotalResult(result *QueryResult) *NodeCPUModeTotalResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	node, _ := result.GetString(KubernetesNodeLabel)
	mode, _ := result.GetString(ModeLabel)

	return &NodeCPUModeTotalResult{
		UID:     uid,
		Cluster: cluster,
		Node:    node,
		Mode:    mode,
		Data:    result.Values,
	}
}

type NodeIsSpotResult struct {
	UID        string
	Cluster    string
	Node       string
	ProviderID string
	Data       []*util.Vector
}

func DecodeNodeIsSpotResult(result *QueryResult) *NodeIsSpotResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	node, _ := result.GetNode()
	providerId, _ := result.GetProviderID()

	return &NodeIsSpotResult{
		UID:        uid,
		Cluster:    cluster,
		Node:       node,
		ProviderID: providerId,
		Data:       result.Values,
	}
}

type NodeRAMSystemPercentResult struct {
	UID      string
	Cluster  string
	Instance string
	Data     []*util.Vector
}

func DecodeNodeRAMSystemPercentResult(result *QueryResult) *NodeRAMSystemPercentResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	instance, _ := result.GetInstance()

	return &NodeRAMSystemPercentResult{
		UID:      uid,
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
	UID       string
	Cluster   string
	Namespace string
	Service   string
	IngressIP string

	Data []*util.Vector
}

func DecodeLBActiveMinutesResult(result *QueryResult) *LBActiveMinutesResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	service, _ := result.GetString(ServiceNameLabel)
	ingressIp, _ := result.GetString(IngressIPLabel)

	return &LBActiveMinutesResult{
		UID:       uid,
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
	UID         string
	Cluster     string
	Provisioner string
	Data        []*util.Vector
}

func DecodeClusterManagementDurationResult(result *QueryResult) *ClusterManagementDurationResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	provisioner, _ := result.GetString(ProvisionerNameLabel)

	return &ClusterManagementDurationResult{
		UID:         uid,
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
	UID       string
	Cluster   string
	Node      string
	Instance  string
	Namespace string
	Pod       string
	Container string

	Data []*util.Vector
}

func DecodeContainerMetricResult(result *QueryResult) *ContainerMetricResult {
	uid, _ := result.GetString(UIDLabel)
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
		UID:       uid,
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

type RAMLimitsResult = ContainerMetricResult

func DecodeRAMLimitsResult(result *QueryResult) *RAMLimitsResult {
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
	UID          string
	Cluster      string
	Node         string
	InstanceType string
	ProviderID   string
	Data         []*util.Vector
}

func DecodeNodeRAMPricePerGiBHrResult(result *QueryResult) *NodeRAMPricePerGiBHrResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	node, _ := result.GetNode()
	instanceType, _ := result.GetInstanceType()
	providerId, _ := result.GetProviderID()

	return &NodeRAMPricePerGiBHrResult{
		UID:          uid,
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

type CPULimitsResult = ContainerMetricResult

func DecodeCPULimitsResult(result *QueryResult) *CPULimitsResult {
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
	UID          string
	Cluster      string
	Node         string
	InstanceType string
	ProviderID   string
	Data         []*util.Vector
}

func DecodeNodeCPUPricePerHrResult(result *QueryResult) *NodeCPUPricePerHrResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	node, _ := result.GetNode()
	instanceType, _ := result.GetInstanceType()
	providerId, _ := result.GetProviderID()

	return &NodeCPUPricePerHrResult{
		UID:          uid,
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
	UID       string
	Cluster   string
	Namespace string
	Pod       string
	Container string
	Data      []*util.Vector
}

func DecodeGPUsAllocatedResult(result *QueryResult) *GPUsAllocatedResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	pod, _ := result.GetPod()
	container, _ := result.GetContainer()

	return &GPUsAllocatedResult{
		UID:       uid,
		Cluster:   cluster,
		Namespace: namespace,
		Pod:       pod,
		Container: container,
		Data:      result.Values,
	}
}

type GPUsUsageAvgResult struct {
	UID       string
	Cluster   string
	Namespace string
	Pod       string
	Container string

	Data []*util.Vector
}

func DecodeGPUsUsageAvgResult(result *QueryResult) *GPUsUsageAvgResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	pod, _ := result.GetPod()
	container, _ := result.GetContainer()

	return &GPUsUsageAvgResult{
		UID:       uid,
		Cluster:   cluster,
		Namespace: namespace,
		Pod:       pod,
		Container: container,
		Data:      result.Values,
	}
}

type GPUsUsageMaxResult struct {
	UID       string
	Cluster   string
	Namespace string
	Pod       string
	Container string
	Data      []*util.Vector
}

func DecodeGPUsUsageMaxResult(result *QueryResult) *GPUsUsageMaxResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	pod, _ := result.GetPod()
	container, _ := result.GetContainer()

	return &GPUsUsageMaxResult{
		UID:       uid,
		Cluster:   cluster,
		Namespace: namespace,
		Pod:       pod,
		Container: container,
		Data:      result.Values,
	}
}

type NodeGPUPricePerHrResult struct {
	UID          string
	Cluster      string
	Node         string
	InstanceType string
	ProviderID   string
	Data         []*util.Vector
}

func DecodeNodeGPUPricePerHrResult(result *QueryResult) *NodeGPUPricePerHrResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	node, _ := result.GetNode()
	instanceType, _ := result.GetInstanceType()
	providerId, _ := result.GetProviderID()

	return &NodeGPUPricePerHrResult{
		UID:          uid,
		Cluster:      cluster,
		Node:         node,
		InstanceType: instanceType,
		ProviderID:   providerId,
		Data:         result.Values,
	}
}

type GPUInfoResult struct {
	UID       string
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
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	pod, _ := result.GetPod()
	container, _ := result.GetContainer()
	device, _ := result.GetString(DeviceLabel)
	modelName, _ := result.GetString(ModelNameLabel)
	uuid, _ := result.GetString(UUIDLabel)

	return &GPUInfoResult{
		UID:       uid,
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
	UID       string
	Cluster   string
	Namespace string
	Pod       string
	Container string
	Resource  string
	Data      []*util.Vector
}

func DecodeIsGPUSharedResult(result *QueryResult) *IsGPUSharedResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	pod, _ := result.GetPod()
	container, _ := result.GetContainer()
	resource, _ := result.GetString(ResourceLabel)

	return &IsGPUSharedResult{
		UID:       uid,
		Cluster:   cluster,
		Namespace: namespace,
		Pod:       pod,
		Container: container,
		Resource:  resource,
		Data:      result.Values,
	}
}

type PodPVCAllocationResult struct {
	UID                   string
	Cluster               string
	Namespace             string
	Pod                   string
	PersistentVolume      string
	PersistentVolumeClaim string
	Data                  []*util.Vector
}

func DecodePodPVCAllocationResult(result *QueryResult) *PodPVCAllocationResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	pod, _ := result.GetPod()
	pv, _ := result.GetString(PVLabel)
	pvc, _ := result.GetString(PVCLabel)

	return &PodPVCAllocationResult{
		UID:                   uid,
		Cluster:               cluster,
		Namespace:             namespace,
		Pod:                   pod,
		PersistentVolume:      pv,
		PersistentVolumeClaim: pvc,
		Data:                  result.Values,
	}
}

type PVCBytesRequestedResult struct {
	UID                   string
	Cluster               string
	Namespace             string
	PersistentVolumeClaim string

	Data []*util.Vector
}

func DecodePVCBytesRequestedResult(result *QueryResult) *PVCBytesRequestedResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	pvc, _ := result.GetString(PVCLabel)

	return &PVCBytesRequestedResult{
		UID:                   uid,
		Cluster:               cluster,
		Namespace:             namespace,
		PersistentVolumeClaim: pvc,
		Data:                  result.Values,
	}
}

type PVCInfoResult struct {
	UID                   string
	Cluster               string
	Namespace             string
	VolumeName            string
	PersistentVolumeClaim string
	StorageClass          string

	Data []*util.Vector
}

func DecodePVCInfoResult(result *QueryResult) *PVCInfoResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	volumeName, _ := result.GetString(VolumeNameLabel)
	pvc, _ := result.GetString(PVCLabel)
	storageClass, _ := result.GetString(StorageClassLabel)

	return &PVCInfoResult{
		UID:                   uid,
		Cluster:               cluster,
		Namespace:             namespace,
		VolumeName:            volumeName,
		PersistentVolumeClaim: pvc,
		StorageClass:          storageClass,
		Data:                  result.Values,
	}
}

type PVBytesResult struct {
	UID              string
	Cluster          string
	PersistentVolume string

	Data []*util.Vector
}

func DecodePVBytesResult(result *QueryResult) *PVBytesResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	pv, _ := result.GetString(PVLabel)

	return &PVBytesResult{
		UID:              uid,
		Cluster:          cluster,
		PersistentVolume: pv,
		Data:             result.Values,
	}
}

type PVPricePerGiBHourResult struct {
	UID              string
	Cluster          string
	VolumeName       string
	PersistentVolume string
	ProviderID       string

	Data []*util.Vector
}

func DecodePVPricePerGiBHourResult(result *QueryResult) *PVPricePerGiBHourResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	volumeName, _ := result.GetString(VolumeNameLabel)
	pv, _ := result.GetString(PVLabel)
	providerId, _ := result.GetProviderID()

	return &PVPricePerGiBHourResult{
		UID:              uid,
		Cluster:          cluster,
		VolumeName:       volumeName,
		PersistentVolume: pv,
		ProviderID:       providerId,

		Data: result.Values,
	}
}

type PVInfoResult struct {
	UID              string
	Cluster          string
	PersistentVolume string
	StorageClass     string
	ProviderID       string

	Data []*util.Vector
}

func DecodePVInfoResult(result *QueryResult) *PVInfoResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	storageClass, _ := result.GetString(StorageClassLabel)
	providerId, _ := result.GetProviderID()
	pv, _ := result.GetString(PVLabel)

	return &PVInfoResult{
		UID:              uid,
		Cluster:          cluster,
		PersistentVolume: pv,
		StorageClass:     storageClass,
		ProviderID:       providerId,
		Data:             result.Values,
	}
}

// Base type for network usage results
type NetworkGiBResult struct {
	UID       string
	Cluster   string
	Namespace string
	Pod       string
	Service   string

	Data []*util.Vector
}

func DecodeNetworkGiBResult(result *QueryResult) *NetworkGiBResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	pod, _ := result.GetPod()
	service, _ := result.GetString(ServiceLabel)

	return &NetworkGiBResult{
		UID:       uid,
		Cluster:   cluster,
		Namespace: namespace,
		Pod:       pod,
		Service:   service,
		Data:      result.Values,
	}
}

// Base type for network price results
type NetworkPricePerGiBResult struct {
	UID     string
	Cluster string

	Data []*util.Vector
}

func DecodeNetworkPricePerGiBResult(result *QueryResult) *NetworkPricePerGiBResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()

	return &NetworkPricePerGiBResult{
		UID:     uid,
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
	UID       string
	Cluster   string
	Namespace string
	Pod       string
	Container string
	Data      []*util.Vector
}

func DecodeNetReceiveBytesResult(result *QueryResult) *NetReceiveBytesResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	pod, _ := result.GetPod()
	container, _ := result.GetContainer()

	return &NetReceiveBytesResult{
		UID:       uid,
		Cluster:   cluster,
		Namespace: namespace,
		Pod:       pod,
		Container: container,
		Data:      result.Values,
	}
}

type NetTransferBytesResult struct {
	UID       string
	Cluster   string
	Namespace string
	Pod       string
	Container string

	Data []*util.Vector
}

func DecodeNetTransferBytesResult(result *QueryResult) *NetTransferBytesResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	pod, _ := result.GetPod()
	container, _ := result.GetContainer()

	return &NetTransferBytesResult{
		UID:       uid,
		Cluster:   cluster,
		Namespace: namespace,
		Pod:       pod,
		Container: container,
		Data:      result.Values,
	}
}

type NamespaceAnnotationsResult struct {
	UID         string
	Cluster     string
	Namespace   string
	Annotations map[string]string

	Data []*util.Vector
}

func DecodeNamespaceAnnotationsResult(result *QueryResult) *NamespaceAnnotationsResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	annotations := result.GetAnnotations()

	return &NamespaceAnnotationsResult{
		UID:         uid,
		Cluster:     cluster,
		Namespace:   namespace,
		Annotations: annotations,
		Data:        result.Values,
	}
}

type PodAnnotationsResult struct {
	UID         string
	Cluster     string
	Namespace   string
	Pod         string
	Annotations map[string]string

	Data []*util.Vector
}

func DecodePodAnnotationsResult(result *QueryResult) *PodAnnotationsResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	pod, _ := result.GetPod()
	annotations := result.GetAnnotations()

	return &PodAnnotationsResult{
		UID:         uid,
		Cluster:     cluster,
		Namespace:   namespace,
		Pod:         pod,
		Annotations: annotations,
		Data:        result.Values,
	}
}

type NodeLabelsResult struct {
	UID     string
	Cluster string
	Node    string
	Labels  map[string]string
	Data    []*util.Vector
}

func DecodeNodeLabelsResult(result *QueryResult) *NodeLabelsResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	node, _ := result.GetNode()
	labels := result.GetLabels()

	return &NodeLabelsResult{
		UID:     uid,
		Cluster: cluster,
		Node:    node,
		Labels:  labels,
		Data:    result.Values,
	}
}

type NamespaceLabelsResult struct {
	UID       string
	Cluster   string
	Namespace string
	Labels    map[string]string
	Data      []*util.Vector
}

func DecodeNamespaceLabelsResult(result *QueryResult) *NamespaceLabelsResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	labels := result.GetLabels()

	return &NamespaceLabelsResult{
		UID:       uid,
		Cluster:   cluster,
		Namespace: namespace,
		Labels:    labels,
		Data:      result.Values,
	}
}

type PodLabelsResult struct {
	UID       string
	Cluster   string
	Namespace string
	Pod       string
	Labels    map[string]string
	Data      []*util.Vector
}

func DecodePodLabelsResult(result *QueryResult) *PodLabelsResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	pod, _ := result.GetPod()
	labels := result.GetLabels()

	return &PodLabelsResult{
		UID:       uid,
		Cluster:   cluster,
		Namespace: namespace,
		Pod:       pod,
		Labels:    labels,
		Data:      result.Values,
	}
}

type ServiceLabelsResult struct {
	UID       string
	Cluster   string
	Namespace string
	Service   string
	Labels    map[string]string

	Data []*util.Vector
}

func DecodeServiceLabelsResult(result *QueryResult) *ServiceLabelsResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	service, _ := result.GetString(ServiceLabel)
	labels := result.GetLabels()

	return &ServiceLabelsResult{
		UID:       uid,
		Cluster:   cluster,
		Namespace: namespace,
		Service:   service,
		Labels:    labels,
		Data:      result.Values,
	}
}

type DeploymentLabelsResult struct {
	UID        string
	Cluster    string
	Namespace  string
	Deployment string
	Labels     map[string]string
	Data       []*util.Vector
}

func DecodeDeploymentLabelsResult(result *QueryResult) *DeploymentLabelsResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	deployment, _ := result.GetString(DeploymentLabel)
	labels := result.GetLabels()

	return &DeploymentLabelsResult{
		UID:        uid,
		Cluster:    cluster,
		Namespace:  namespace,
		Deployment: deployment,
		Labels:     labels,
		Data:       result.Values,
	}
}

type StatefulSetLabelsResult struct {
	UID         string
	Cluster     string
	Namespace   string
	StatefulSet string
	Labels      map[string]string
	Data        []*util.Vector
}

func DecodeStatefulSetLabelsResult(result *QueryResult) *StatefulSetLabelsResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	statefulSet, _ := result.GetString(StatefulSetLabel)
	labels := result.GetLabels()

	return &StatefulSetLabelsResult{
		UID:         uid,
		Cluster:     cluster,
		Namespace:   namespace,
		StatefulSet: statefulSet,
		Labels:      labels,
		Data:        result.Values,
	}
}

type DaemonSetLabelsResult struct {
	UID       string
	Cluster   string
	Namespace string
	Pod       string
	DaemonSet string
	Labels    map[string]string
	Data      []*util.Vector
}

func DecodeDaemonSetLabelsResult(result *QueryResult) *DaemonSetLabelsResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	pod, _ := result.GetPod()
	daemonSet, _ := result.GetString(OwnerNameLabel)
	labels := result.GetLabels()

	return &DaemonSetLabelsResult{
		UID:       uid,
		Cluster:   cluster,
		Namespace: namespace,
		Pod:       pod,
		DaemonSet: daemonSet,
		Labels:    labels,
		Data:      result.Values,
	}
}

type JobLabelsResult struct {
	UID       string
	Cluster   string
	Namespace string
	Pod       string
	Job       string
	Labels    map[string]string
	Data      []*util.Vector
}

func DecodeJobLabelsResult(result *QueryResult) *JobLabelsResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	pod, _ := result.GetPod()
	job, _ := result.GetString(OwnerNameLabel)
	labels := result.GetLabels()

	return &JobLabelsResult{
		UID:       uid,
		Cluster:   cluster,
		Namespace: namespace,
		Pod:       pod,
		Job:       job,
		Labels:    labels,
		Data:      result.Values,
	}
}

type PodsWithReplicaSetOwnerResult struct {
	UID        string
	Cluster    string
	Namespace  string
	Pod        string
	ReplicaSet string

	Data []*util.Vector
}

func DecodePodsWithReplicaSetOwnerResult(result *QueryResult) *PodsWithReplicaSetOwnerResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	replicaSet, _ := result.GetString(OwnerNameLabel)
	pod, _ := result.GetPod()

	return &PodsWithReplicaSetOwnerResult{
		UID:        uid,
		Cluster:    cluster,
		Namespace:  namespace,
		Pod:        pod,
		ReplicaSet: replicaSet,
		Data:       result.Values,
	}
}

type ReplicaSetsWithoutOwnersResult struct {
	UID        string
	Cluster    string
	Namespace  string
	ReplicaSet string

	Data []*util.Vector
}

func DecodeReplicaSetsWithoutOwnersResult(result *QueryResult) *ReplicaSetsWithoutOwnersResult {
	uid, _ := result.GetString(UIDLabel)
	return &ReplicaSetsWithoutOwnersResult{
		UID:  uid,
		Data: result.Values,
	}
}

type ReplicaSetsWithRolloutResult struct {
	UID        string
	Cluster    string
	Namespace  string
	ReplicaSet string
	OwnerName  string
	OwnerKind  string
	Data       []*util.Vector
}

func DecodeReplicaSetsWithRolloutResult(result *QueryResult) *ReplicaSetsWithRolloutResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	replicaSet, _ := result.GetString(ReplicaSetLabel)
	ownerName, _ := result.GetString(OwnerNameLabel)
	ownerKind, _ := result.GetString(OwnerKindLabel)

	return &ReplicaSetsWithRolloutResult{
		UID:        uid,
		Cluster:    cluster,
		Namespace:  namespace,
		ReplicaSet: replicaSet,
		OwnerName:  ownerName,
		OwnerKind:  ownerKind,
		Data:       result.Values,
	}
}

type ResourceQuotaMetricResult struct {
	UID           string
	Namespace     string
	ResourceQuota string
	Resource      string
	Unit          string
	Data          []*util.Vector
}

func DecodeResourceQuotaMetricResult(result *QueryResult) *ResourceQuotaMetricResult {
	uid, _ := result.GetString(UIDLabel)
	namespace, _ := result.GetNamespace()
	resourceQuota, _ := result.GetString(ResourceQuotaLabel)
	resource, _ := result.GetString(ResourceLabel)
	unit, _ := result.GetString(UnitLabel)

	return &ResourceQuotaMetricResult{
		UID:           uid,
		Namespace:     namespace,
		ResourceQuota: resourceQuota,
		Resource:      resource,
		Unit:          unit,
		Data:          result.Values,
	}
}

type ResourceQuotaSpecCPURequestAvgResult = ResourceQuotaMetricResult

func DecodeResourceQuotaSpecCPURequestAvgResult(result *QueryResult) *ResourceQuotaSpecCPURequestAvgResult {
	return DecodeResourceQuotaMetricResult(result)
}

type ResourceQuotaSpecCPURequestMaxResult = ResourceQuotaMetricResult

func DecodeResourceQuotaSpecCPURequestMaxResult(result *QueryResult) *ResourceQuotaSpecCPURequestMaxResult {
	return DecodeResourceQuotaMetricResult(result)
}

type ResourceQuotaSpecRAMRequestAvgResult = ResourceQuotaMetricResult

func DecodeResourceQuotaSpecRAMRequestAvgResult(result *QueryResult) *ResourceQuotaSpecRAMRequestAvgResult {
	return DecodeResourceQuotaMetricResult(result)
}

type ResourceQuotaSpecRAMRequestMaxResult = ResourceQuotaMetricResult

func DecodeResourceQuotaSpecRAMRequestMaxResult(result *QueryResult) *ResourceQuotaSpecRAMRequestMaxResult {
	return DecodeResourceQuotaMetricResult(result)
}

type ResourceQuotaSpecCPULimitAvgResult = ResourceQuotaMetricResult

func DecodeResourceQuotaSpecCPULimitAvgResult(result *QueryResult) *ResourceQuotaSpecCPULimitAvgResult {
	return DecodeResourceQuotaMetricResult(result)
}

type ResourceQuotaSpecCPULimitMaxResult = ResourceQuotaMetricResult

func DecodeResourceQuotaSpecCPULimitMaxResult(result *QueryResult) *ResourceQuotaSpecCPULimitMaxResult {
	return DecodeResourceQuotaMetricResult(result)
}

type ResourceQuotaSpecRAMLimitAvgResult = ResourceQuotaMetricResult

func DecodeResourceQuotaSpecRAMLimitAvgResult(result *QueryResult) *ResourceQuotaSpecRAMLimitAvgResult {
	return DecodeResourceQuotaMetricResult(result)
}

type ResourceQuotaSpecRAMLimitMaxResult = ResourceQuotaMetricResult

func DecodeResourceQuotaSpecRAMLimitMaxResult(result *QueryResult) *ResourceQuotaSpecRAMLimitMaxResult {
	return DecodeResourceQuotaMetricResult(result)
}

type ResourceQuotaStatusUsedCPURequestAvgResult = ResourceQuotaMetricResult

func DecodeResourceQuotaStatusUsedCPURequestAvgResult(result *QueryResult) *ResourceQuotaStatusUsedCPURequestAvgResult {
	return DecodeResourceQuotaMetricResult(result)
}

type ResourceQuotaStatusUsedCPURequestMaxResult = ResourceQuotaMetricResult

func DecodeResourceQuotaStatusUsedCPURequestMaxResult(result *QueryResult) *ResourceQuotaStatusUsedCPURequestMaxResult {
	return DecodeResourceQuotaMetricResult(result)
}

type ResourceQuotaStatusUsedRAMRequestAvgResult = ResourceQuotaMetricResult

func DecodeResourceQuotaStatusUsedRAMRequestAvgResult(result *QueryResult) *ResourceQuotaStatusUsedRAMRequestAvgResult {
	return DecodeResourceQuotaMetricResult(result)
}

type ResourceQuotaStatusUsedRAMRequestMaxResult = ResourceQuotaMetricResult

func DecodeResourceQuotaStatusUsedRAMRequestMaxResult(result *QueryResult) *ResourceQuotaStatusUsedRAMRequestMaxResult {
	return DecodeResourceQuotaMetricResult(result)
}

type ResourceQuotaStatusUsedCPULimitAvgResult = ResourceQuotaMetricResult

func DecodeResourceQuotaStatusUsedCPULimitAvgResult(result *QueryResult) *ResourceQuotaStatusUsedCPULimitAvgResult {
	return DecodeResourceQuotaMetricResult(result)
}

type ResourceQuotaStatusUsedCPULimitMaxResult = ResourceQuotaMetricResult

func DecodeResourceQuotaStatusUsedCPULimitMaxResult(result *QueryResult) *ResourceQuotaStatusUsedCPULimitMaxResult {
	return DecodeResourceQuotaMetricResult(result)
}

type ResourceQuotaStatusUsedRAMLimitAvgResult = ResourceQuotaMetricResult

func DecodeResourceQuotaStatusUsedRAMLimitAvgResult(result *QueryResult) *ResourceQuotaStatusUsedRAMLimitAvgResult {
	return DecodeResourceQuotaMetricResult(result)
}

type ResourceQuotaStatusUsedRAMLimitMaxResult = ResourceQuotaMetricResult

func DecodeResourceQuotaStatusUsedRAMLimitMaxResult(result *QueryResult) *ResourceQuotaStatusUsedRAMLimitMaxResult {
	return DecodeResourceQuotaMetricResult(result)
}

func DecodeAll[T any](results []*QueryResult, decode ResultDecoder[T]) []*T {
	decoded := make([]*T, 0, len(results))
	for _, result := range results {
		decoded = append(decoded, decode(result))
	}

	return decoded
}
