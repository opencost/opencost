package clustercache

import (
	"sync"

	cc "github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/pkg/env"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	stv1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
)

// KubernetesClusterCache is the implementation of ClusterCache
type KubernetesClusterCache struct {
	client kubernetes.Interface

	namespaceWatch             WatchController
	nodeWatch                  WatchController
	podWatch                   WatchController
	serviceWatch               WatchController
	daemonsetsWatch            WatchController
	deploymentsWatch           WatchController
	statefulsetWatch           WatchController
	replicasetWatch            WatchController
	pvWatch                    WatchController
	pvcWatch                   WatchController
	storageClassWatch          WatchController
	jobsWatch                  WatchController
	pdbWatch                   WatchController
	replicationControllerWatch WatchController
	stop                       chan struct{}
}

func initializeCache(wc WatchController, wg *sync.WaitGroup, cancel chan struct{}) {
	defer wg.Done()
	wc.WarmUp(cancel)
}

func NewKubernetesClusterCache(client kubernetes.Interface) cc.ClusterCache {
	if env.GetUseCacheV1() {
		return NewKubernetesClusterCacheV1(client)
	}
	return NewKubernetesClusterCacheV2(client)
}

func NewKubernetesClusterCacheV1(client kubernetes.Interface) cc.ClusterCache {
	coreRestClient := client.CoreV1().RESTClient()
	appsRestClient := client.AppsV1().RESTClient()
	storageRestClient := client.StorageV1().RESTClient()
	batchClient := client.BatchV1().RESTClient()
	pdbClient := client.PolicyV1().RESTClient()

	installNamespace := env.GetOpencostNamespace()
	log.Infof("NAMESPACE: %s", installNamespace)

	kcc := &KubernetesClusterCache{
		client:                     client,
		namespaceWatch:             NewCachingWatcher(coreRestClient, "namespaces", &v1.Namespace{}, "", fields.Everything()),
		nodeWatch:                  NewCachingWatcher(coreRestClient, "nodes", &v1.Node{}, "", fields.Everything()),
		podWatch:                   NewCachingWatcher(coreRestClient, "pods", &v1.Pod{}, "", fields.Everything()),
		serviceWatch:               NewCachingWatcher(coreRestClient, "services", &v1.Service{}, "", fields.Everything()),
		daemonsetsWatch:            NewCachingWatcher(appsRestClient, "daemonsets", &appsv1.DaemonSet{}, "", fields.Everything()),
		deploymentsWatch:           NewCachingWatcher(appsRestClient, "deployments", &appsv1.Deployment{}, "", fields.Everything()),
		statefulsetWatch:           NewCachingWatcher(appsRestClient, "statefulsets", &appsv1.StatefulSet{}, "", fields.Everything()),
		replicasetWatch:            NewCachingWatcher(appsRestClient, "replicasets", &appsv1.ReplicaSet{}, "", fields.Everything()),
		pvWatch:                    NewCachingWatcher(coreRestClient, "persistentvolumes", &v1.PersistentVolume{}, "", fields.Everything()),
		pvcWatch:                   NewCachingWatcher(coreRestClient, "persistentvolumeclaims", &v1.PersistentVolumeClaim{}, "", fields.Everything()),
		storageClassWatch:          NewCachingWatcher(storageRestClient, "storageclasses", &stv1.StorageClass{}, "", fields.Everything()),
		jobsWatch:                  NewCachingWatcher(batchClient, "jobs", &batchv1.Job{}, "", fields.Everything()),
		pdbWatch:                   NewCachingWatcher(pdbClient, "poddisruptionbudgets", &policyv1.PodDisruptionBudget{}, "", fields.Everything()),
		replicationControllerWatch: NewCachingWatcher(coreRestClient, "replicationcontrollers", &v1.ReplicationController{}, "", fields.Everything()),
	}

	// Wait for each caching watcher to initialize
	cancel := make(chan struct{})
	var wg sync.WaitGroup
	if env.HasKubernetesResourceAccess() {
		wg.Add(14)
		go initializeCache(kcc.namespaceWatch, &wg, cancel)
		go initializeCache(kcc.nodeWatch, &wg, cancel)
		go initializeCache(kcc.podWatch, &wg, cancel)
		go initializeCache(kcc.serviceWatch, &wg, cancel)
		go initializeCache(kcc.daemonsetsWatch, &wg, cancel)
		go initializeCache(kcc.deploymentsWatch, &wg, cancel)
		go initializeCache(kcc.statefulsetWatch, &wg, cancel)
		go initializeCache(kcc.replicasetWatch, &wg, cancel)
		go initializeCache(kcc.pvWatch, &wg, cancel)
		go initializeCache(kcc.pvcWatch, &wg, cancel)
		go initializeCache(kcc.storageClassWatch, &wg, cancel)
		go initializeCache(kcc.jobsWatch, &wg, cancel)
		go initializeCache(kcc.pdbWatch, &wg, cancel)
		go initializeCache(kcc.replicationControllerWatch, &wg, cancel)
	}

	wg.Wait()

	log.Infof("Done waiting")

	return kcc
}

func (kcc *KubernetesClusterCache) Run() {
	if kcc.stop != nil {
		return
	}
	stopCh := make(chan struct{})

	go kcc.namespaceWatch.Run(1, stopCh)
	go kcc.nodeWatch.Run(1, stopCh)
	go kcc.podWatch.Run(1, stopCh)
	go kcc.serviceWatch.Run(1, stopCh)
	go kcc.daemonsetsWatch.Run(1, stopCh)
	go kcc.deploymentsWatch.Run(1, stopCh)
	go kcc.statefulsetWatch.Run(1, stopCh)
	go kcc.replicasetWatch.Run(1, stopCh)
	go kcc.pvWatch.Run(1, stopCh)
	go kcc.pvcWatch.Run(1, stopCh)
	go kcc.storageClassWatch.Run(1, stopCh)
	go kcc.jobsWatch.Run(1, stopCh)
	go kcc.pdbWatch.Run(1, stopCh)
	go kcc.replicationControllerWatch.Run(1, stopCh)

	kcc.stop = stopCh
}

func (kcc *KubernetesClusterCache) Stop() {
	if kcc.stop == nil {
		return
	}

	close(kcc.stop)
	kcc.stop = nil
}

func (kcc *KubernetesClusterCache) GetAllNamespaces() []*cc.Namespace {
	var namespaces []*cc.Namespace
	items := kcc.namespaceWatch.GetAll()
	for _, ns := range items {
		namespaces = append(namespaces, cc.TransformNamespace(ns.(*v1.Namespace)))
	}
	return namespaces
}

func (kcc *KubernetesClusterCache) GetAllNodes() []*cc.Node {
	var nodes []*cc.Node
	items := kcc.nodeWatch.GetAll()
	for _, node := range items {
		nodes = append(nodes, cc.TransformNode(node.(*v1.Node)))
	}
	return nodes
}

func (kcc *KubernetesClusterCache) GetAllPods() []*cc.Pod {
	var pods []*cc.Pod
	items := kcc.podWatch.GetAll()
	for _, pod := range items {
		pods = append(pods, cc.TransformPod(pod.(*v1.Pod)))
	}
	return pods
}

func (kcc *KubernetesClusterCache) GetAllServices() []*cc.Service {
	var services []*cc.Service
	items := kcc.serviceWatch.GetAll()
	for _, service := range items {
		services = append(services, cc.TransformService(service.(*v1.Service)))
	}
	return services
}

func (kcc *KubernetesClusterCache) GetAllDaemonSets() []*cc.DaemonSet {
	var daemonsets []*cc.DaemonSet
	items := kcc.daemonsetsWatch.GetAll()
	for _, daemonset := range items {
		daemonsets = append(daemonsets, cc.TransformDaemonSet(daemonset.(*appsv1.DaemonSet)))
	}
	return daemonsets
}

func (kcc *KubernetesClusterCache) GetAllDeployments() []*cc.Deployment {
	var deployments []*cc.Deployment
	items := kcc.deploymentsWatch.GetAll()
	for _, deployment := range items {
		deployments = append(deployments, cc.TransformDeployment(deployment.(*appsv1.Deployment)))
	}
	return deployments
}

func (kcc *KubernetesClusterCache) GetAllStatefulSets() []*cc.StatefulSet {
	var statefulsets []*cc.StatefulSet
	items := kcc.statefulsetWatch.GetAll()
	for _, statefulset := range items {
		statefulsets = append(statefulsets, cc.TransformStatefulSet(statefulset.(*appsv1.StatefulSet)))
	}
	return statefulsets
}

func (kcc *KubernetesClusterCache) GetAllReplicaSets() []*cc.ReplicaSet {
	var replicasets []*cc.ReplicaSet
	items := kcc.replicasetWatch.GetAll()
	for _, replicaset := range items {
		replicasets = append(replicasets, cc.TransformReplicaSet(replicaset.(*appsv1.ReplicaSet)))
	}
	return replicasets
}

func (kcc *KubernetesClusterCache) GetAllPersistentVolumes() []*cc.PersistentVolume {
	var pvs []*cc.PersistentVolume
	items := kcc.pvWatch.GetAll()
	for _, pv := range items {
		pvs = append(pvs, cc.TransformPersistentVolume(pv.(*v1.PersistentVolume)))
	}
	return pvs
}

func (kcc *KubernetesClusterCache) GetAllPersistentVolumeClaims() []*cc.PersistentVolumeClaim {
	var pvcs []*cc.PersistentVolumeClaim
	items := kcc.pvcWatch.GetAll()
	for _, pvc := range items {
		pvcs = append(pvcs, cc.TransformPersistentVolumeClaim(pvc.(*v1.PersistentVolumeClaim)))
	}
	return pvcs
}

func (kcc *KubernetesClusterCache) GetAllStorageClasses() []*cc.StorageClass {
	var storageClasses []*cc.StorageClass
	items := kcc.storageClassWatch.GetAll()
	for _, stc := range items {
		storageClasses = append(storageClasses, cc.TransformStorageClass(stc.(*stv1.StorageClass)))
	}
	return storageClasses
}

func (kcc *KubernetesClusterCache) GetAllJobs() []*cc.Job {
	var jobs []*cc.Job
	items := kcc.jobsWatch.GetAll()
	for _, job := range items {
		jobs = append(jobs, cc.TransformJob(job.(*batchv1.Job)))
	}
	return jobs
}

func (kcc *KubernetesClusterCache) GetAllPodDisruptionBudgets() []*cc.PodDisruptionBudget {
	var pdbs []*cc.PodDisruptionBudget
	items := kcc.pdbWatch.GetAll()
	for _, pdb := range items {
		pdbs = append(pdbs, cc.TransformPodDisruptionBudget(pdb.(*policyv1.PodDisruptionBudget)))
	}
	return pdbs
}

func (kcc *KubernetesClusterCache) GetAllReplicationControllers() []*cc.ReplicationController {
	var rcs []*cc.ReplicationController
	items := kcc.replicationControllerWatch.GetAll()
	for _, rc := range items {
		rcs = append(rcs, cc.TransformReplicationController(rc.(*v1.ReplicationController)))
	}
	return rcs
}
