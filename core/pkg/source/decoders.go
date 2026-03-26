package source

import (
	"time"

	"github.com/opencost/opencost/core/pkg/util"
)

const (
	ProviderLabel        = "provider"
	AccountIDLabel       = "account_id"
	ClusterNameLabel     = "cluster_name"
	RegionLabel          = "region"
	ClusterIDLabel       = "cluster_id"
	NamespaceLabel       = "namespace"
	NamespaceUIDLabel    = "namespace_uid"
	NodeLabel            = "node"
	NodeUIDLabel         = "node_uid"
	InstanceLabel        = "instance"
	InstanceTypeLabel    = "instance_type"
	ContainerLabel       = "container"
	PodLabel             = "pod"
	PodUIDLabel          = "pod_uid"
	PodNameLabel         = "pod_name"
	PodVolumeNameLabel   = "pod_volume_name"
	ProviderIDLabel      = "provider_id"
	DeviceLabel          = "device"
	PVCLabel             = "persistentvolumeclaim"
	PVCUIDLabel          = "persistentvolumeclaim_uid"
	PVLabel              = "persistentvolume"
	CSIVolumeHandleLabel = "csi_volume_handle"
	StorageClassLabel    = "storageclass"
	VolumeNameLabel      = "volumename"
	PVUIDLabel           = "persistentvolume_uid"
	ServiceLabel         = "service"
	ServiceNameLabel     = "service_name"
	ServiceTypeLabel     = "service_type"
	IngressIPLabel       = "ingress_ip"
	ProvisionerNameLabel = "provisioner_name"
	UIDLabel             = "uid"
	KubernetesNodeLabel  = "kubernetes_node"
	ModeLabel            = "mode"
	ModelNameLabel       = "modelName"
	HostNameLabel        = "Hostname"
	UUIDLabel            = "UUID"
	ResourceLabel        = "resource"
	DeploymentLabel      = "deployment"
	StatefulSetLabel     = "statefulSet"
	DaemonSetLabel       = "daemonset"
	JobLabel             = "job"
	CronJobLabel         = "cronjob"
	ReplicaSetLabel      = "replicaset"
	ResourceQuotaLabel   = "resourcequota"
	OwnerNameLabel       = "owner_name"
	OwnerKindLabel       = "owner_kind"
	OwnerUIDLabel        = "owner_uid"
	ControllerLabel      = "controller"
	UnitLabel            = "unit"
	InternetLabel        = "internet"
	SameZoneLabel        = "same_zone"
	SameRegionLabel      = "same_region"
	NatGatewayLabel      = "nat_gateway"
)

const (
	NoneLabelValue = "<none>"
)

// UptimeResult represents the first and last recorded sample timestamp within the query window
type UptimeResult struct {
	UID   string
	First time.Time
	Last  time.Time
}

func (res *UptimeResult) GetStartEnd(windowStart, windowEnd time.Time, resolution time.Duration) (time.Time, time.Time) {
	return getStartEnd(res.First, res.Last, windowStart, windowEnd, resolution)
}

func DecodeUptimeResult(result *QueryResult) *UptimeResult {
	uid, _ := result.GetString(UIDLabel)
	first := time.Unix(int64(result.Values[0].Timestamp), 0).UTC()
	last := time.Unix(int64(result.Values[len(result.Values)-1].Timestamp), 0).UTC()

	return &UptimeResult{
		UID:   uid,
		First: first,
		Last:  last,
	}
}

// Container requires some special results because container name and pod UID is required to uniqly identify it

type ContainerUptimeResult struct {
	UptimeResult
	Container string
}

func DecodeContainerUptimeResult(result *QueryResult) *ContainerUptimeResult {
	container, _ := result.GetString(ContainerLabel)
	ur := DecodeUptimeResult(result)
	return &ContainerUptimeResult{
		UptimeResult: *ur,
		Container:    container,
	}
}

type ContainerResourceResult struct {
	ResourceResult
	Container string
}

func DecodeContainerResourceResult(result *QueryResult) *ContainerResourceResult {
	container, _ := result.GetString(ContainerLabel)
	rr := DecodeResourceResult(result)
	return &ContainerResourceResult{
		ResourceResult: *rr,
		Container:      container,
	}
}

type LabelsResult struct {
	UID     string
	Cluster string
	Labels  map[string]string
}

func DecodeLabelsResult(result *QueryResult) *LabelsResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	labels := result.GetLabels()

	return &LabelsResult{
		UID:     uid,
		Cluster: cluster,
		Labels:  labels,
	}
}

type AnnotationsResult struct {
	UID         string
	Cluster     string
	Annotations map[string]string
}

func DecodeAnnotationsResult(result *QueryResult) *AnnotationsResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	annotations := result.GetAnnotations()

	return &AnnotationsResult{
		UID:         uid,
		Cluster:     cluster,
		Annotations: annotations,
	}
}

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

type NodeInfoResult struct {
	UID          string
	Cluster      string
	Node         string
	ProviderID   string
	InstanceType string
}

func DecodeNodeInfoResult(result *QueryResult) *NodeInfoResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	node, _ := result.GetNode()
	providerId, _ := result.GetProviderID()
	instanceType, _ := result.GetInstanceType()

	return &NodeInfoResult{
		UID:          uid,
		Cluster:      cluster,
		Node:         node,
		ProviderID:   providerId,
		InstanceType: instanceType,
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
	UID      string
	Cluster  string
	Node     string
	CPUCores float64
}

func DecodeNodeCPUCoresCapacityResult(result *QueryResult) *NodeCPUCoresCapacityResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	node, _ := result.GetNode()

	return &NodeCPUCoresCapacityResult{
		UID:      uid,
		Cluster:  cluster,
		Node:     node,
		CPUCores: result.Values[0].Value,
	}
}

type NodeCPUCoresAllocatableResult = NodeCPUCoresCapacityResult

func DecodeNodeCPUCoresAllocatableResult(result *QueryResult) *NodeCPUCoresAllocatableResult {
	return DecodeNodeCPUCoresCapacityResult(result)
}

type NodeRAMBytesCapacityResult struct {
	UID      string
	Cluster  string
	Node     string
	RAMBytes float64
}

func DecodeNodeRAMBytesCapacityResult(result *QueryResult) *NodeRAMBytesCapacityResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	node, _ := result.GetNode()
	bytes := result.Values[0].Value

	return &NodeRAMBytesCapacityResult{
		UID:      uid,
		Cluster:  cluster,
		Node:     node,
		RAMBytes: bytes,
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
	GPUCount   float64
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
		GPUCount:   result.Values[0].Value,
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

type ClusterInfoResult struct {
	UID         string
	Cluster     string
	Provider    string
	AccountID   string
	Provisioner string
	Region      string
}

func DecodeClusterInfoResult(result *QueryResult) *ClusterInfoResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetString(ClusterNameLabel)
	provider, _ := result.GetString(ProviderLabel)
	accountID, _ := result.GetString(AccountIDLabel)
	provisioner, _ := result.GetString(ProvisionerNameLabel)
	region, _ := result.GetString(RegionLabel)

	return &ClusterInfoResult{
		UID:         uid,
		Cluster:     cluster,
		Provider:    provider,
		AccountID:   accountID,
		Provisioner: provisioner,
		Region:      region,
	}
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

type PodInfoResult struct {
	UID          string
	Cluster      string
	Pod          string
	NamespaceUID string
	NodeUID      string
}

func DecodePodInfoResult(result *QueryResult) *PodInfoResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	pod, _ := result.GetPod()
	namespaceUID, _ := result.GetString(NamespaceUIDLabel)
	nodeUID, _ := result.GetString(NodeUIDLabel)

	return &PodInfoResult{
		UID:          uid,
		Cluster:      cluster,
		Pod:          pod,
		NamespaceUID: namespaceUID,
		NodeUID:      nodeUID,
	}
}

type PodPVCVolumeResult struct {
	UID           string
	Cluster       string
	PVCUID        string
	PodVolumeName string
}

func DecodePodPVCVolumeResult(result *QueryResult) *PodPVCVolumeResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	pvcUID, _ := result.GetString(PVCUIDLabel)
	podVolumeName, _ := result.GetString(PodVolumeNameLabel)

	return &PodPVCVolumeResult{
		UID:           uid,
		Cluster:       cluster,
		PVCUID:        pvcUID,
		PodVolumeName: podVolumeName,
	}
}

type OwnerResult struct {
	UID        string
	Cluster    string
	OwnerUID   string
	OwnerKind  string
	Controller bool
}

func DecodeOwnerResult(result *QueryResult) *OwnerResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	ownerUID, _ := result.GetString(OwnerUIDLabel)
	ownerKind, _ := result.GetString(OwnerKindLabel)
	controllerStr, _ := result.GetString(ControllerLabel)
	controller := false
	if controllerStr == "true" {
		controller = true
	}

	return &OwnerResult{
		UID:        uid,
		Cluster:    cluster,
		OwnerUID:   ownerUID,
		OwnerKind:  ownerKind,
		Controller: controller,
	}
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
	uid, _ := result.GetString(PodUIDLabel)
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
	NamespaceUID          string
	Namespace             string
	VolumeName            string
	PVUID                 string
	PersistentVolumeClaim string
	StorageClass          string
	Data                  []*util.Vector
}

func DecodePVCInfoResult(result *QueryResult) *PVCInfoResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	namespaceUID, _ := result.GetString(NamespaceUIDLabel)
	namespace, _ := result.GetNamespace()
	pvUID, _ := result.GetString(PVUIDLabel)
	volumeName, _ := result.GetString(VolumeNameLabel)
	pvc, _ := result.GetString(PVCLabel)
	storageClass, _ := result.GetString(StorageClassLabel)

	return &PVCInfoResult{
		UID:                   uid,
		Cluster:               cluster,
		NamespaceUID:          namespaceUID,
		Namespace:             namespace,
		PVUID:                 pvUID,
		VolumeName:            volumeName,
		PersistentVolumeClaim: pvc,
		StorageClass:          storageClass,

		Data: result.Values,
	}
}

type PVBytesResult struct {
	UID              string
	Cluster          string
	PersistentVolume string
	Value            float64
}

func DecodePVBytesResult(result *QueryResult) *PVBytesResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	pv, _ := result.GetString(PVLabel)
	var value float64
	if len(result.Values) > 0 {
		value = result.Values[0].Value
	}

	return &PVBytesResult{
		UID:              uid,
		Cluster:          cluster,
		PersistentVolume: pv,
		Value:            value,
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
	CSIVolumeHandle  string
	Data             []*util.Vector
}

func DecodePVInfoResult(result *QueryResult) *PVInfoResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	storageClass, _ := result.GetString(StorageClassLabel)
	providerId, _ := result.GetProviderID()
	pv, _ := result.GetString(PVLabel)
	csiVolumeHandle, _ := result.GetString(CSIVolumeHandleLabel)

	return &PVInfoResult{
		UID:              uid,
		Cluster:          cluster,
		PersistentVolume: pv,
		StorageClass:     storageClass,
		ProviderID:       providerId,
		CSIVolumeHandle:  csiVolumeHandle,

		Data: result.Values,
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

type NetNatGatewayPricePerGiBResult = NetworkPricePerGiBResult
type NetNatGatewayGiBResult = NetworkGiBResult

type NetNatGatewayZoneGiBResult = NetworkGiBResult
type NetNatGatewayRegionGiBResult = NetworkGiBResult
type NetNatGatewayInternetGiBResult = NetworkGiBResult

type NetZoneIngressGiBResult = NetworkGiBResult
type NetRegionIngressGiBResult = NetworkGiBResult
type NetInternetIngressGiBResult = NetworkGiBResult
type NetInternetServiceIngressGiBResult = NetworkGiBResult
type NetNatGatewayIngressGiBResult = NetworkGiBResult

type NetNatGatewayZoneIngressGiBResult = NetworkGiBResult
type NetNatGatewayRegionIngressGiBResult = NetworkGiBResult
type NetNatGatewayInternetIngressGiBResult = NetworkGiBResult

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

func DecodeNetNatGatewayPricePerGiBResult(result *QueryResult) *NetNatGatewayPricePerGiBResult {
	return DecodeNetworkPricePerGiBResult(result)
}

func DecodeNetNatGatewayGiBResult(result *QueryResult) *NetNatGatewayGiBResult {
	return DecodeNetworkGiBResult(result)
}

func DecodeNetNatGatewayZoneGiBResult(result *QueryResult) *NetNatGatewayZoneGiBResult {
	return DecodeNetworkGiBResult(result)
}

func DecodeNetNatGatewayRegionGiBResult(result *QueryResult) *NetNatGatewayRegionGiBResult {
	return DecodeNetworkGiBResult(result)
}

func DecodeNetNatGatewayInternetGiBResult(result *QueryResult) *NetNatGatewayInternetGiBResult {
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

func DecodeNetNatGatewayIngressGiBResult(result *QueryResult) *NetNatGatewayIngressGiBResult {
	return DecodeNetworkGiBResult(result)
}

func DecodeNetNatGatewayZoneIngressGiBResult(result *QueryResult) *NetNatGatewayZoneIngressGiBResult {
	return DecodeNetworkGiBResult(result)
}

func DecodeNetNatGatewayRegionIngressGiBResult(result *QueryResult) *NetNatGatewayRegionIngressGiBResult {
	return DecodeNetworkGiBResult(result)
}

func DecodeNetNatGatewayInternetIngressGiBResult(result *QueryResult) *NetNatGatewayInternetIngressGiBResult {
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
	}
}

type NamespaceInfoResult struct {
	UID       string
	Cluster   string
	Namespace string
}

func DecodeNamespaceInfoResult(result *QueryResult) *NamespaceInfoResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()

	return &NamespaceInfoResult{
		UID:       uid,
		Cluster:   cluster,
		Namespace: namespace,
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

type ServiceInfoResult struct {
	UID          string
	Cluster      string
	NamespaceUID string
	Service      string
	ServiceType  string
}

func DecodeServiceInfoResult(result *QueryResult) *ServiceInfoResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	namespaceUID, _ := result.GetString(NamespaceUIDLabel)
	service, _ := result.GetString(ServiceLabel)
	serviceType, _ := result.GetString(ServiceTypeLabel)

	return &ServiceInfoResult{
		UID:          uid,
		Cluster:      cluster,
		NamespaceUID: namespaceUID,
		Service:      service,
		ServiceType:  serviceType,
	}
}

type DeploymentInfoResult struct {
	UID          string
	Cluster      string
	NamespaceUID string
	Deployment   string
}

func DecodeDeploymentInfoResult(result *QueryResult) *DeploymentInfoResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	namespaceUID, _ := result.GetString(NamespaceUIDLabel)
	deployment, _ := result.GetString(DeploymentLabel)

	return &DeploymentInfoResult{
		UID:          uid,
		Cluster:      cluster,
		NamespaceUID: namespaceUID,
		Deployment:   deployment,
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

type StatefulSetInfoResult struct {
	UID          string
	Cluster      string
	NamespaceUID string
	StatefulSet  string
}

func DecodeStatefulSetInfoResult(result *QueryResult) *StatefulSetInfoResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	namespaceUID, _ := result.GetString(NamespaceUIDLabel)
	statefulSet, _ := result.GetString(StatefulSetLabel)

	return &StatefulSetInfoResult{
		UID:          uid,
		Cluster:      cluster,
		NamespaceUID: namespaceUID,
		StatefulSet:  statefulSet,
	}
}

type DaemonSetInfoResult struct {
	UID          string
	Cluster      string
	NamespaceUID string
	DaemonSet    string
}

func DecodeDaemonSetInfoResult(result *QueryResult) *DaemonSetInfoResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	namespaceUID, _ := result.GetString(NamespaceUIDLabel)
	daemonSet, _ := result.GetString(DaemonSetLabel)

	return &DaemonSetInfoResult{
		UID:          uid,
		Cluster:      cluster,
		NamespaceUID: namespaceUID,
		DaemonSet:    daemonSet,
	}
}

type JobInfoResult struct {
	UID          string
	Cluster      string
	NamespaceUID string
	Job          string
}

func DecodeJobInfoResult(result *QueryResult) *JobInfoResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	namespaceUID, _ := result.GetString(NamespaceUIDLabel)
	job, _ := result.GetString(JobLabel)

	return &JobInfoResult{
		UID:          uid,
		Cluster:      cluster,
		NamespaceUID: namespaceUID,
		Job:          job,
	}
}

type CronJobInfoResult struct {
	UID          string
	Cluster      string
	NamespaceUID string
	CronJob      string
}

func DecodeCronJobInfoResult(result *QueryResult) *CronJobInfoResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	namespaceUID, _ := result.GetString(NamespaceUIDLabel)
	cronJob, _ := result.GetString(CronJobLabel)

	return &CronJobInfoResult{
		UID:          uid,
		Cluster:      cluster,
		NamespaceUID: namespaceUID,
		CronJob:      cronJob,
	}
}

type ReplicaSetInfoResult struct {
	UID          string
	Cluster      string
	NamespaceUID string
	ReplicaSet   string
}

func DecodeReplicaSetInfoResult(result *QueryResult) *ReplicaSetInfoResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	namespaceUID, _ := result.GetString(NamespaceUIDLabel)
	replicaSet, _ := result.GetString(ReplicaSetLabel)

	return &ReplicaSetInfoResult{
		UID:          uid,
		Cluster:      cluster,
		NamespaceUID: namespaceUID,
		ReplicaSet:   replicaSet,
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

type PodsWithDaemonSetOwnerResult struct {
	UID       string
	Cluster   string
	Namespace string
	Pod       string
	DaemonSet string
}

func DecodePodsWithDaemonSetOwnerResult(result *QueryResult) *PodsWithDaemonSetOwnerResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	pod, _ := result.GetPod()
	daemonSet, _ := result.GetString(OwnerNameLabel)

	return &PodsWithDaemonSetOwnerResult{
		UID:       uid,
		Cluster:   cluster,
		Namespace: namespace,
		Pod:       pod,
		DaemonSet: daemonSet,
	}
}

type PodsWithJobOwnerResult struct {
	UID       string
	Cluster   string
	Namespace string
	Pod       string
	Job       string
}

func DecodePodsWithJobOwnerResult(result *QueryResult) *PodsWithJobOwnerResult {
	uid, _ := result.GetString(UIDLabel)
	cluster, _ := result.GetCluster()
	namespace, _ := result.GetNamespace()
	pod, _ := result.GetPod()
	job, _ := result.GetString(OwnerNameLabel)

	return &PodsWithJobOwnerResult{
		UID:       uid,
		Cluster:   cluster,
		Namespace: namespace,
		Pod:       pod,
		Job:       job,
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

type ResourceQuotaInfoResult struct {
	UID           string
	NamespaceUID  string
	ResourceQuota string
}

func DecodeResourceQuotaInfoResult(result *QueryResult) *ResourceQuotaInfoResult {
	uid, _ := result.GetString(UIDLabel)
	namespaceUID, _ := result.GetString(NamespaceUIDLabel)
	resourceQuota, _ := result.GetString(ResourceQuotaLabel)

	return &ResourceQuotaInfoResult{
		UID:           uid,
		NamespaceUID:  namespaceUID,
		ResourceQuota: resourceQuota,
	}
}

type ResourceResult struct {
	UID      string
	Resource string
	Unit     string
	Value    float64
}

func DecodeResourceResult(result *QueryResult) *ResourceResult {
	uid, _ := result.GetString(UIDLabel)
	resource, _ := result.GetString(ResourceLabel)
	unit, _ := result.GetString(UnitLabel)
	var value float64
	if len(result.Values) > 0 {
		value = result.Values[0].Value
	}

	return &ResourceResult{
		UID:      uid,
		Resource: resource,
		Unit:     unit,
		Value:    value,
	}
}

// DCGM needs specialized results because it uses UUID instead of the uid label that we use.
type DCGMDeviceInfoResult struct {
	UUID      string
	Device    string
	ModelName string
	HostName  string
}

func DecodeDCGMDeviceInfoResult(result *QueryResult) *DCGMDeviceInfoResult {
	uuid, _ := result.GetString(UUIDLabel)
	device, _ := result.GetString(DeviceLabel)
	modelName, _ := result.GetString(ModelNameLabel)
	hostName, _ := result.GetString(HostNameLabel)

	return &DCGMDeviceInfoResult{
		UUID:      uuid,
		Device:    device,
		ModelName: modelName,
		HostName:  hostName,
	}
}

type DCGMDeviceUptimeResult struct {
	UUID  string
	First time.Time
	Last  time.Time
}

func (res *DCGMDeviceUptimeResult) GetStartEnd(windowStart, windowEnd time.Time, resolution time.Duration) (time.Time, time.Time) {
	return getStartEnd(res.First, res.Last, windowStart, windowEnd, resolution)
}

func getStartEnd(first, last, windowStart, windowEnd time.Time, resolution time.Duration) (time.Time, time.Time) {
	// The only corner-case here is what to do if you only get one timestamp.
	// This dilemma still requires the use of the resolution, and can be
	// clamped using the window. In this case, we want to honor the existence
	// of the pod by giving "one resolution" worth of duration, half on each
	// side of the given timestamp.
	if first.Equal(last) {
		first = first.Add(-1 * resolution / time.Duration(2))
		last = last.Add(resolution / time.Duration(2))
	}
	if first.Before(windowStart) {
		first = windowStart
	}
	if last.After(windowEnd) {
		last = windowEnd
	}
	// prevent end times in the future
	now := time.Now().UTC()
	if last.After(now) {
		last = now
	}

	return first, last
}

func DecodeDCGMDeviceUptimeResult(result *QueryResult) *DCGMDeviceUptimeResult {
	uuid, _ := result.GetString(UUIDLabel)
	first := time.Unix(int64(result.Values[0].Timestamp), 0).UTC()
	last := time.Unix(int64(result.Values[len(result.Values)-1].Timestamp), 0).UTC()

	return &DCGMDeviceUptimeResult{
		UUID:  uuid,
		First: first,
		Last:  last,
	}
}

type DCGMDeviceContainerUsageResult struct {
	UUID      string
	PodUID    string
	Container string
	Value     float64
}

func DecodeDCGMDeviceContainerUsageResult(result *QueryResult) *DCGMDeviceContainerUsageResult {
	uuid, _ := result.GetString(UUIDLabel)
	podUID, _ := result.GetString(PodUIDLabel)
	container, _ := result.GetString(ContainerLabel)
	var value float64
	if len(result.Values) > 0 {
		value = result.Values[0].Value
	}

	return &DCGMDeviceContainerUsageResult{
		UUID:      uuid,
		PodUID:    podUID,
		Container: container,
		Value:     value,
	}
}

func DecodeAll[T any](results []*QueryResult, decode ResultDecoder[T]) []*T {
	decoded := make([]*T, 0, len(results))
	for _, result := range results {
		decoded = append(decoded, decode(result))
	}

	return decoded
}
