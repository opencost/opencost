package clustercache

import (
	"sync"

	cc "github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/pkg/env"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	resourcev1 "k8s.io/api/resource/v1"
	stv1 "k8s.io/api/storage/v1"
	"k8s.io/client-go/kubernetes"
)

type KubernetesClusterCacheV2 struct {
	namespaceStore             *GenericStore[*v1.Namespace, *cc.Namespace]
	nodeStore                  *GenericStore[*v1.Node, *cc.Node]
	podStore                   *GenericStore[*v1.Pod, *cc.Pod]
	serviceStore               *GenericStore[*v1.Service, *cc.Service]
	daemonSetStore             *GenericStore[*appsv1.DaemonSet, *cc.DaemonSet]
	deploymentStore            *GenericStore[*appsv1.Deployment, *cc.Deployment]
	statefulSetStore           *GenericStore[*appsv1.StatefulSet, *cc.StatefulSet]
	persistentVolumeStore      *GenericStore[*v1.PersistentVolume, *cc.PersistentVolume]
	persistentVolumeClaimStore *GenericStore[*v1.PersistentVolumeClaim, *cc.PersistentVolumeClaim]
	storageClassStore          *GenericStore[*stv1.StorageClass, *cc.StorageClass]
	jobStore                   *GenericStore[*batchv1.Job, *cc.Job]
	cronJobStore               *GenericStore[*batchv1.CronJob, *cc.CronJob]
	replicationControllerStore *GenericStore[*v1.ReplicationController, *cc.ReplicationController]
	replicaSetStore            *GenericStore[*appsv1.ReplicaSet, *cc.ReplicaSet]
	pdbStore                   *GenericStore[*policyv1.PodDisruptionBudget, *cc.PodDisruptionBudget]
	resourceQuotasStore        *GenericStore[*v1.ResourceQuota, *cc.ResourceQuota]
	// DRA stores are nil when the cluster does not serve resource.k8s.io/v1
	resourceSliceStore *GenericStore[*resourcev1.ResourceSlice, *cc.ResourceSlice]
	resourceClaimStore *GenericStore[*resourcev1.ResourceClaim, *cc.ResourceClaim]
	stopCh             chan struct{}
}

func NewKubernetesClusterCacheV2(clientset kubernetes.Interface) *KubernetesClusterCacheV2 {
	kcc := &KubernetesClusterCacheV2{
		namespaceStore:             CreateStore(clientset.CoreV1().RESTClient(), "namespaces", cc.TransformNamespace),
		nodeStore:                  CreateStore(clientset.CoreV1().RESTClient(), "nodes", cc.TransformNode),
		persistentVolumeClaimStore: CreateStore(clientset.CoreV1().RESTClient(), "persistentvolumeclaims", cc.TransformPersistentVolumeClaim),
		persistentVolumeStore:      CreateStore(clientset.CoreV1().RESTClient(), "persistentvolumes", cc.TransformPersistentVolume),
		podStore:                   CreateStore(clientset.CoreV1().RESTClient(), "pods", cc.TransformPod),
		replicationControllerStore: CreateStore(clientset.CoreV1().RESTClient(), "replicationcontrollers", cc.TransformReplicationController),
		serviceStore:               CreateStore(clientset.CoreV1().RESTClient(), "services", cc.TransformService),
		daemonSetStore:             CreateStore(clientset.AppsV1().RESTClient(), "daemonsets", cc.TransformDaemonSet),
		deploymentStore:            CreateStore(clientset.AppsV1().RESTClient(), "deployments", cc.TransformDeployment),
		replicaSetStore:            CreateStore(clientset.AppsV1().RESTClient(), "replicasets", cc.TransformReplicaSet),
		statefulSetStore:           CreateStore(clientset.AppsV1().RESTClient(), "statefulsets", cc.TransformStatefulSet),
		storageClassStore:          CreateStore(clientset.StorageV1().RESTClient(), "storageclasses", cc.TransformStorageClass),
		jobStore:                   CreateStore(clientset.BatchV1().RESTClient(), "jobs", cc.TransformJob),
		cronJobStore:               CreateStore(clientset.BatchV1().RESTClient(), "cronjobs", cc.TransformCronJob),
		pdbStore:                   CreateStore(clientset.PolicyV1().RESTClient(), "poddisruptionbudgets", cc.TransformPodDisruptionBudget),
		resourceQuotasStore:        CreateStore(clientset.CoreV1().RESTClient(), "resourcequotas", cc.TransformResourceQuota),
		stopCh:                     make(chan struct{}),
	}

	// DRA (resource.k8s.io/v1) is optional: only create the stores when the
	// API is served, so clusters without DRA see no reflector error spam
	// and the GetAll methods return nil
	if draAPIAvailable(clientset) {
		kcc.resourceSliceStore = CreateStore(clientset.ResourceV1().RESTClient(), "resourceslices", cc.TransformResourceSlice)
		kcc.resourceClaimStore = CreateStore(clientset.ResourceV1().RESTClient(), "resourceclaims", cc.TransformResourceClaim)
	}

	return kcc
}

func (kcc *KubernetesClusterCacheV2) Run() {
	var wg sync.WaitGroup

	if env.HasKubernetesResourceAccess() {
		wg.Add(16)
		kcc.namespaceStore.Watch(kcc.stopCh, wg.Done)
		kcc.nodeStore.Watch(kcc.stopCh, wg.Done)
		kcc.persistentVolumeClaimStore.Watch(kcc.stopCh, wg.Done)
		kcc.persistentVolumeStore.Watch(kcc.stopCh, wg.Done)
		kcc.podStore.Watch(kcc.stopCh, wg.Done)
		kcc.replicationControllerStore.Watch(kcc.stopCh, wg.Done)
		kcc.serviceStore.Watch(kcc.stopCh, wg.Done)
		kcc.daemonSetStore.Watch(kcc.stopCh, wg.Done)
		kcc.deploymentStore.Watch(kcc.stopCh, wg.Done)
		kcc.replicaSetStore.Watch(kcc.stopCh, wg.Done)
		kcc.statefulSetStore.Watch(kcc.stopCh, wg.Done)
		kcc.storageClassStore.Watch(kcc.stopCh, wg.Done)
		kcc.jobStore.Watch(kcc.stopCh, wg.Done)
		kcc.cronJobStore.Watch(kcc.stopCh, wg.Done)
		kcc.pdbStore.Watch(kcc.stopCh, wg.Done)
		kcc.resourceQuotasStore.Watch(kcc.stopCh, wg.Done)
		if kcc.resourceSliceStore != nil {
			wg.Add(2)
			kcc.resourceSliceStore.Watch(kcc.stopCh, wg.Done)
			kcc.resourceClaimStore.Watch(kcc.stopCh, wg.Done)
		}
	}
	wg.Wait()
}

func (kcc *KubernetesClusterCacheV2) Stop() {
	if kcc.stopCh != nil {
		close(kcc.stopCh)

		kcc.stopCh = nil
	}
}

func (kcc *KubernetesClusterCacheV2) GetAllNamespaces() []*cc.Namespace {
	return kcc.namespaceStore.GetAll()
}

func (kcc *KubernetesClusterCacheV2) GetAllNodes() []*cc.Node {
	return kcc.nodeStore.GetAll()
}

func (kcc *KubernetesClusterCacheV2) GetAllPods() []*cc.Pod {
	return kcc.podStore.GetAll()
}

func (kcc *KubernetesClusterCacheV2) GetAllServices() []*cc.Service {
	return kcc.serviceStore.GetAll()
}

func (kcc *KubernetesClusterCacheV2) GetAllDaemonSets() []*cc.DaemonSet {
	return kcc.daemonSetStore.GetAll()
}

func (kcc *KubernetesClusterCacheV2) GetAllDeployments() []*cc.Deployment {
	return kcc.deploymentStore.GetAll()
}

func (kcc *KubernetesClusterCacheV2) GetAllStatefulSets() []*cc.StatefulSet {
	return kcc.statefulSetStore.GetAll()
}

func (kcc *KubernetesClusterCacheV2) GetAllPersistentVolumes() []*cc.PersistentVolume {
	return kcc.persistentVolumeStore.GetAll()
}

func (kcc *KubernetesClusterCacheV2) GetAllPersistentVolumeClaims() []*cc.PersistentVolumeClaim {
	return kcc.persistentVolumeClaimStore.GetAll()
}

func (kcc *KubernetesClusterCacheV2) GetAllStorageClasses() []*cc.StorageClass {
	return kcc.storageClassStore.GetAll()
}

func (kcc *KubernetesClusterCacheV2) GetAllJobs() []*cc.Job {
	return kcc.jobStore.GetAll()
}

func (kcc *KubernetesClusterCacheV2) GetAllCronJobs() []*cc.CronJob {
	return kcc.cronJobStore.GetAll()
}

func (kcc *KubernetesClusterCacheV2) GetAllReplicationControllers() []*cc.ReplicationController {
	return kcc.replicationControllerStore.GetAll()
}

func (kcc *KubernetesClusterCacheV2) GetAllReplicaSets() []*cc.ReplicaSet {
	return kcc.replicaSetStore.GetAll()
}

func (kcc *KubernetesClusterCacheV2) GetAllPodDisruptionBudgets() []*cc.PodDisruptionBudget {
	return kcc.pdbStore.GetAll()
}

func (kcc *KubernetesClusterCacheV2) GetAllResourceQuotas() []*cc.ResourceQuota {
	return kcc.resourceQuotasStore.GetAll()
}

func (kcc *KubernetesClusterCacheV2) GetAllResourceSlices() []*cc.ResourceSlice {
	if kcc.resourceSliceStore == nil {
		return nil
	}
	return kcc.resourceSliceStore.GetAll()
}

func (kcc *KubernetesClusterCacheV2) GetAllResourceClaims() []*cc.ResourceClaim {
	if kcc.resourceClaimStore == nil {
		return nil
	}
	return kcc.resourceClaimStore.GetAll()
}
