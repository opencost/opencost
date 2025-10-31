package clustercache

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	stv1 "k8s.io/api/storage/v1"
)

type Namespace struct {
	UID         types.UID
	Name        string
	Labels      map[string]string
	Annotations map[string]string
}

type Pod struct {
	UID               types.UID
	Name              string
	Namespace         string
	Labels            map[string]string
	Annotations       map[string]string
	OwnerReferences   []metav1.OwnerReference
	Status            PodStatus
	Spec              PodSpec
	DeletionTimestamp *time.Time
}

type PodStatus struct {
	PodIP             string
	Phase             v1.PodPhase
	ContainerStatuses []v1.ContainerStatus
}

type PodSpec struct {
	NodeName      string
	Containers    []Container
	Volumes       []v1.Volume
	RestartPolicy v1.RestartPolicy
}

type Container struct {
	Name      string
	Resources v1.ResourceRequirements
}

type Node struct {
	UID            types.UID
	Name           string
	Labels         map[string]string
	Annotations    map[string]string
	Status         v1.NodeStatus
	SpecProviderID string
}

type Service struct {
	UID          types.UID
	Name         string
	Namespace    string
	SpecSelector map[string]string
	Type         v1.ServiceType
	Status       v1.ServiceStatus
	ClusterIP    string
}

type DaemonSet struct {
	Name           string
	Namespace      string
	Labels         map[string]string
	SpecContainers []v1.Container
}

type Deployment struct {
	UID                     types.UID
	Name                    string
	Namespace               string
	Labels                  map[string]string
	Annotations             map[string]string
	MatchLabels             map[string]string
	SpecSelector            *metav1.LabelSelector
	SpecReplicas            *int32
	SpecStrategy            appsv1.DeploymentStrategy
	StatusAvailableReplicas int32
	PodSpec                 PodSpec
}

type StatefulSet struct {
	UID          types.UID
	Name         string
	Namespace    string
	Labels       map[string]string
	Annotations  map[string]string
	SpecSelector *metav1.LabelSelector
	SpecReplicas *int32
	PodSpec      PodSpec
}

type PersistentVolumeClaim struct {
	UID         types.UID
	Name        string
	Namespace   string
	Spec        v1.PersistentVolumeClaimSpec
	Labels      map[string]string
	Annotations map[string]string
}

type StorageClass struct {
	Name        string
	Labels      map[string]string
	Annotations map[string]string
	Parameters  map[string]string
	Provisioner string
	TypeMeta    metav1.TypeMeta
	Size        int
}

type Job struct {
	UID       types.UID
	Name      string
	Namespace string
	Status    batchv1.JobStatus
}

type PersistentVolume struct {
	UID         types.UID
	Name        string
	Namespace   string
	Labels      map[string]string
	Annotations map[string]string
	Spec        v1.PersistentVolumeSpec
	Status      v1.PersistentVolumeStatus
}

type ReplicationController struct {
	Name      string
	Namespace string
	Spec      v1.ReplicationControllerSpec
}

type PodDisruptionBudget struct {
	Name      string
	Namespace string
	Spec      policyv1.PodDisruptionBudgetSpec
	Status    policyv1.PodDisruptionBudgetStatus
}

type ReplicaSet struct {
	UID             types.UID
	Name            string
	Namespace       string
	OwnerReferences []metav1.OwnerReference
	SpecSelector    *metav1.LabelSelector
	Spec            appsv1.ReplicaSetSpec
}

type ResourceQuota struct {
	UID       types.UID
	Name      string
	Namespace string
	Spec      v1.ResourceQuotaSpec
	Status    v1.ResourceQuotaStatus
}

type Volume struct {
}

// GetControllerOf returns a pointer to a copy of the controllerRef if controllee has a controller
func GetControllerOf(pod *Pod) *metav1.OwnerReference {
	ref := GetControllerOfNoCopy(pod)
	if ref == nil {
		return nil
	}
	cp := *ref
	cp.Controller = ptr.To(*ref.Controller)
	if ref.BlockOwnerDeletion != nil {
		cp.BlockOwnerDeletion = ptr.To(*ref.BlockOwnerDeletion)
	}
	return &cp
}

// GetControllerOfNoCopy returns a pointer to the controllerRef if controllee has a controller
func GetControllerOfNoCopy(pod *Pod) *metav1.OwnerReference {
	refs := pod.OwnerReferences
	for i := range refs {
		if refs[i].Controller != nil && *refs[i].Controller {
			return &refs[i]
		}
	}
	return nil
}

func TransformNamespace(input *v1.Namespace) *Namespace {
	return &Namespace{
		UID:         input.UID,
		Name:        input.Name,
		Annotations: input.Annotations,
		Labels:      input.Labels,
	}
}

func TransformPodContainer(input v1.Container) Container {
	return Container{
		Name:      input.Name,
		Resources: input.Resources,
	}
}

func TransformPodStatus(input v1.PodStatus) PodStatus {
	return PodStatus{
		PodIP:             input.PodIP,
		Phase:             input.Phase,
		ContainerStatuses: input.ContainerStatuses,
	}
}

func TransformPodSpec(input v1.PodSpec) PodSpec {
	containers := make([]Container, len(input.Containers))
	for i, container := range input.Containers {
		containers[i] = TransformPodContainer(container)
	}
	return PodSpec{
		NodeName:      input.NodeName,
		Containers:    containers,
		Volumes:       input.Volumes,
		RestartPolicy: input.RestartPolicy,
	}

}

func TransformTimestamp(input *metav1.Time) *time.Time {
	if input == nil {
		return nil
	}

	t := input.Time
	return &t
}

func TransformPod(input *v1.Pod) *Pod {
	return &Pod{
		UID:               input.UID,
		Name:              input.Name,
		Namespace:         input.Namespace,
		Labels:            input.Labels,
		Annotations:       input.Annotations,
		OwnerReferences:   input.OwnerReferences,
		Spec:              TransformPodSpec(input.Spec),
		Status:            TransformPodStatus(input.Status),
		DeletionTimestamp: TransformTimestamp(input.DeletionTimestamp),
	}
}

func TransformNode(input *v1.Node) *Node {
	return &Node{
		UID:            input.UID,
		Name:           input.Name,
		Labels:         input.Labels,
		Annotations:    input.Annotations,
		Status:         input.Status,
		SpecProviderID: input.Spec.ProviderID,
	}
}

func TransformService(input *v1.Service) *Service {
	return &Service{
		UID:          input.UID,
		Name:         input.Name,
		Namespace:    input.Namespace,
		SpecSelector: input.Spec.Selector,
		Type:         input.Spec.Type,
		Status:       input.Status,
		ClusterIP:    input.Spec.ClusterIP,
	}
}

func TransformDaemonSet(input *appsv1.DaemonSet) *DaemonSet {
	return &DaemonSet{
		Name:           input.Name,
		Namespace:      input.Namespace,
		Labels:         input.Labels,
		SpecContainers: input.Spec.Template.Spec.Containers,
	}
}

func TransformDeployment(input *appsv1.Deployment) *Deployment {
	return &Deployment{
		UID:                     input.UID,
		Name:                    input.Name,
		Namespace:               input.Namespace,
		Labels:                  input.Labels,
		MatchLabels:             input.Spec.Selector.MatchLabels,
		SpecReplicas:            input.Spec.Replicas,
		SpecSelector:            input.Spec.Selector,
		SpecStrategy:            input.Spec.Strategy,
		StatusAvailableReplicas: input.Status.AvailableReplicas,
		PodSpec:                 TransformPodSpec(input.Spec.Template.Spec),
	}
}

func TransformStatefulSet(input *appsv1.StatefulSet) *StatefulSet {
	return &StatefulSet{
		Name:         input.Name,
		Namespace:    input.Namespace,
		SpecSelector: input.Spec.Selector,
		SpecReplicas: input.Spec.Replicas,
		PodSpec:      TransformPodSpec(input.Spec.Template.Spec),
		Labels:       input.Labels,
		Annotations:  input.Annotations,
		UID:          input.UID,
	}
}

func TransformPersistentVolume(input *v1.PersistentVolume) *PersistentVolume {
	return &PersistentVolume{
		UID:         input.UID,
		Name:        input.Name,
		Namespace:   input.Namespace,
		Labels:      input.Labels,
		Annotations: input.Annotations,
		Spec:        input.Spec,
		Status:      input.Status,
	}
}

func TransformPersistentVolumeClaim(input *v1.PersistentVolumeClaim) *PersistentVolumeClaim {
	return &PersistentVolumeClaim{
		UID:         input.UID,
		Name:        input.Name,
		Namespace:   input.Namespace,
		Spec:        input.Spec,
		Labels:      input.Labels,
		Annotations: input.Annotations,
	}
}

func TransformStorageClass(input *stv1.StorageClass) *StorageClass {
	return &StorageClass{
		Name:        input.Name,
		Annotations: input.Annotations,
		Labels:      input.Labels,
		Parameters:  input.Parameters,
		Provisioner: input.Provisioner,
		TypeMeta:    input.TypeMeta,
		Size:        input.Size(),
	}
}

func TransformJob(input *batchv1.Job) *Job {
	return &Job{
		UID:       input.UID,
		Name:      input.Name,
		Namespace: input.Namespace,
		Status:    input.Status,
	}
}

func TransformReplicationController(input *v1.ReplicationController) *ReplicationController {
	return &ReplicationController{
		Name:      input.Name,
		Namespace: input.Namespace,
		Spec:      input.Spec,
	}
}

func TransformPodDisruptionBudget(input *policyv1.PodDisruptionBudget) *PodDisruptionBudget {
	return &PodDisruptionBudget{
		Name:      input.Name,
		Namespace: input.Namespace,
		Spec:      input.Spec,
		Status:    input.Status,
	}
}

func TransformReplicaSet(input *appsv1.ReplicaSet) *ReplicaSet {
	return &ReplicaSet{
		UID:             input.UID,
		Name:            input.Name,
		Namespace:       input.Namespace,
		OwnerReferences: input.OwnerReferences,
		Spec:            input.Spec,
		SpecSelector:    input.Spec.Selector,
	}
}

func TransformResourceQuota(input *v1.ResourceQuota) *ResourceQuota {
	return &ResourceQuota{
		UID:       input.UID,
		Name:      input.Name,
		Namespace: input.Namespace,
		Spec:      input.Spec,
		Status:    input.Status,
	}
}

// ClusterCache defines an contract for an object which caches components within a cluster, ensuring
// up to date resources using watchers
type ClusterCache interface {
	// Run starts the watcher processes
	Run()

	// Stops the watcher processes
	Stop()

	// GetAllNamespaces returns all the cached namespaces
	GetAllNamespaces() []*Namespace

	// GetAllNodes returns all the cached nodes
	GetAllNodes() []*Node

	// GetAllPods returns all the cached pods
	GetAllPods() []*Pod

	// GetAllServices returns all the cached services
	GetAllServices() []*Service

	// GetAllDaemonSets returns all the cached DaemonSets
	GetAllDaemonSets() []*DaemonSet

	// GetAllDeployments returns all the cached deployments
	GetAllDeployments() []*Deployment

	// GetAllStatfulSets returns all the cached StatefulSets
	GetAllStatefulSets() []*StatefulSet

	// GetAllReplicaSets returns all the cached ReplicaSets
	GetAllReplicaSets() []*ReplicaSet

	// GetAllPersistentVolumes returns all the cached persistent volumes
	GetAllPersistentVolumes() []*PersistentVolume

	// GetAllPersistentVolumeClaims returns all the cached persistent volume claims
	GetAllPersistentVolumeClaims() []*PersistentVolumeClaim

	// GetAllStorageClasses returns all the cached storage classes
	GetAllStorageClasses() []*StorageClass

	// GetAllJobs returns all the cached jobs
	GetAllJobs() []*Job

	// GetAllPodDisruptionBudgets returns all cached pod disruption budgets
	GetAllPodDisruptionBudgets() []*PodDisruptionBudget

	// GetAllReplicationControllers returns all cached replication controllers
	GetAllReplicationControllers() []*ReplicationController

	// GetAllResourceQuotas returns all cached resource quotas
	GetAllResourceQuotas() []*ResourceQuota
}
