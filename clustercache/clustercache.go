package clustercache

import (
	"sync"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	stv1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
)

// ClusterCache defines an contract for an object which caches components within a cluster, ensuring
// up to date resources using watchers
type ClusterCache interface {
	// Run starts the watcher processes
	Run()

	// Stops the watcher processes
	Stop()

	// Gets the underlying clientset
	// TODO: Remove once we support all cached cluster components
	GetClient() kubernetes.Interface

	// GetAllNamespaces returns all the cached namespaces
	GetAllNamespaces() []*v1.Namespace

	// GetAllNodes returns all the cached nodes
	GetAllNodes() []*v1.Node

	// GetAllPods returns all the cached pods
	GetAllPods() []*v1.Pod

	// GetAllServices returns all the cached services
	GetAllServices() []*v1.Service

	// GetAllDeployments returns all the cached deployments
	GetAllDeployments() []*appsv1.Deployment

	// GetAllPersistentVolumes returns all the cached persistent volumes
	GetAllPersistentVolumes() []*v1.PersistentVolume

	// GetAllStorageClasses returns all the cached storage classes
	GetAllStorageClasses() []*stv1.StorageClass
}

// KubernetesClusterCache is the implementation of ClusterCache
type KubernetesClusterCache struct {
	client kubernetes.Interface

	namespaceWatch    WatchController
	nodeWatch         WatchController
	podWatch          WatchController
	serviceWatch      WatchController
	deploymentsWatch  WatchController
	pvWatch           WatchController
	storageClassWatch WatchController
	stop              chan struct{}
}

func initializeCache(wc WatchController, wg *sync.WaitGroup, cancel chan struct{}) {
	defer wg.Done()
	wc.WarmUp(cancel)
}

func NewKubernetesClusterCache(client kubernetes.Interface) ClusterCache {
	coreRestClient := client.CoreV1().RESTClient()
	appsRestClient := client.AppsV1().RESTClient()
	storageRestClient := client.StorageV1().RESTClient()

	kcc := &KubernetesClusterCache{
		client:            client,
		namespaceWatch:    NewCachingWatcher(coreRestClient, "namespaces", &v1.Namespace{}, "", fields.Everything()),
		nodeWatch:         NewCachingWatcher(coreRestClient, "nodes", &v1.Node{}, "", fields.Everything()),
		podWatch:          NewCachingWatcher(coreRestClient, "pods", &v1.Pod{}, "", fields.Everything()),
		serviceWatch:      NewCachingWatcher(coreRestClient, "services", &v1.Service{}, "", fields.Everything()),
		deploymentsWatch:  NewCachingWatcher(appsRestClient, "deployments", &appsv1.Deployment{}, "", fields.Everything()),
		pvWatch:           NewCachingWatcher(coreRestClient, "persistentvolumes", &v1.PersistentVolume{}, "", fields.Everything()),
		storageClassWatch: NewCachingWatcher(storageRestClient, "storageclasses", &stv1.StorageClass{}, "", fields.Everything()),
	}

	// Wait for each caching watcher to initialize
	var wg sync.WaitGroup
	wg.Add(7)

	cancel := make(chan struct{})

	go initializeCache(kcc.namespaceWatch, &wg, cancel)
	go initializeCache(kcc.nodeWatch, &wg, cancel)
	go initializeCache(kcc.podWatch, &wg, cancel)
	go initializeCache(kcc.serviceWatch, &wg, cancel)
	go initializeCache(kcc.deploymentsWatch, &wg, cancel)
	go initializeCache(kcc.pvWatch, &wg, cancel)
	go initializeCache(kcc.storageClassWatch, &wg, cancel)

	wg.Wait()

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
	go kcc.deploymentsWatch.Run(1, stopCh)
	go kcc.pvWatch.Run(1, stopCh)
	go kcc.storageClassWatch.Run(1, stopCh)

	kcc.stop = stopCh
}

func (kcc *KubernetesClusterCache) Stop() {
	if kcc.stop == nil {
		return
	}

	close(kcc.stop)
	kcc.stop = nil
}

func (kcc *KubernetesClusterCache) GetClient() kubernetes.Interface {
	return kcc.client
}

func (kcc *KubernetesClusterCache) GetAllNamespaces() []*v1.Namespace {
	var namespaces []*v1.Namespace
	items := kcc.namespaceWatch.GetAll()
	for _, ns := range items {
		namespaces = append(namespaces, ns.(*v1.Namespace))
	}
	return namespaces
}

func (kcc *KubernetesClusterCache) GetAllNodes() []*v1.Node {
	var nodes []*v1.Node
	items := kcc.nodeWatch.GetAll()
	for _, node := range items {
		nodes = append(nodes, node.(*v1.Node))
	}
	return nodes
}

func (kcc *KubernetesClusterCache) GetAllPods() []*v1.Pod {
	var pods []*v1.Pod
	items := kcc.podWatch.GetAll()
	for _, pod := range items {
		pods = append(pods, pod.(*v1.Pod))
	}
	return pods
}

func (kcc *KubernetesClusterCache) GetAllServices() []*v1.Service {
	var services []*v1.Service
	items := kcc.serviceWatch.GetAll()
	for _, service := range items {
		services = append(services, service.(*v1.Service))
	}
	return services
}

func (kcc *KubernetesClusterCache) GetAllDeployments() []*appsv1.Deployment {
	var deployments []*appsv1.Deployment
	items := kcc.deploymentsWatch.GetAll()
	for _, deployment := range items {
		deployments = append(deployments, deployment.(*appsv1.Deployment))
	}
	return deployments
}

func (kcc *KubernetesClusterCache) GetAllPersistentVolumes() []*v1.PersistentVolume {
	var pvs []*v1.PersistentVolume
	items := kcc.pvWatch.GetAll()
	for _, pv := range items {
		pvs = append(pvs, pv.(*v1.PersistentVolume))
	}
	return pvs
}

func (kcc *KubernetesClusterCache) GetAllStorageClasses() []*stv1.StorageClass {
	var storageClasses []*stv1.StorageClass
	items := kcc.storageClassWatch.GetAll()
	for _, stc := range items {
		storageClasses = append(storageClasses, stc.(*stv1.StorageClass))
	}
	return storageClasses
}
