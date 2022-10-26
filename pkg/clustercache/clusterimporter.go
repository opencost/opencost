package clustercache

import (
	"sync"

	"github.com/opencost/opencost/pkg/config"
	"github.com/opencost/opencost/pkg/log"
	"github.com/opencost/opencost/pkg/util/json"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/api/policy/v1beta1"
	stv1 "k8s.io/api/storage/v1"
)

// ClusterImporter is an implementation of ClusterCache which leverages a backing configuration file
// as it's source of the cluster data.
type ClusterImporter struct {
	source          *config.ConfigFile
	sourceHandlerID config.HandlerID
	dataLock        *sync.Mutex
	data            *clusterEncoding
}

// Creates a new ClusterCache implementation which uses an import process to provide cluster data
func NewClusterImporter(source *config.ConfigFile) ClusterCache {
	return &ClusterImporter{
		source:   source,
		dataLock: new(sync.Mutex),
		data:     new(clusterEncoding),
	}
}

// onImportSourceChanged handles the source data updating
func (ci *ClusterImporter) onImportSourceChanged(changeType config.ChangeType, data []byte) {
	if changeType == config.ChangeTypeDeleted {
		ci.dataLock.Lock()
		ci.data = new(clusterEncoding)
		ci.dataLock.Unlock()
		return
	}

	ci.update(data)
}

// update replaces the underlying cluster data with the provided new data if it decodes
func (ci *ClusterImporter) update(data []byte) {
	ce := new(clusterEncoding)
	err := json.Unmarshal(data, ce)
	if err != nil {
		log.Warnf("Failed to unmarshal cluster during import: %s", err)
		return
	}

	ci.dataLock.Lock()
	ci.data = ce
	ci.dataLock.Unlock()
}

// Run starts the watcher processes
func (ci *ClusterImporter) Run() {
	if ci.source == nil {
		log.Errorf("ClusterImporter source does not exist, not running")
		return
	}

	exists, err := ci.source.Exists()
	if err != nil {
		log.Errorf("Failed to import source for cluster: %s", err)
		return
	}

	if exists {
		data, err := ci.source.Read()
		if err != nil {
			log.Warnf("Failed to import cluster: %s", err)
		} else {
			ci.update(data)
		}
	}

	ci.sourceHandlerID = ci.source.AddChangeHandler(ci.onImportSourceChanged)
}

// Stops the watcher processes
func (ci *ClusterImporter) Stop() {
	if ci.sourceHandlerID != "" {
		ci.source.RemoveChangeHandler(ci.sourceHandlerID)
		ci.sourceHandlerID = ""
	}
}

// GetAllNamespaces returns all the cached namespaces
func (ci *ClusterImporter) GetAllNamespaces() []*v1.Namespace {
	ci.dataLock.Lock()
	defer ci.dataLock.Unlock()

	// Deep copy here to avoid callers from corrupting the cache
	// This also mimics the behavior of the default cluster cache impl.
	namespaces := ci.data.Namespaces
	cloneList := make([]*v1.Namespace, 0, len(namespaces))
	for _, v := range namespaces {
		cloneList = append(cloneList, v.DeepCopy())
	}
	return cloneList
}

// GetAllNodes returns all the cached nodes
func (ci *ClusterImporter) GetAllNodes() []*v1.Node {
	ci.dataLock.Lock()
	defer ci.dataLock.Unlock()

	// Deep copy here to avoid callers from corrupting the cache
	// This also mimics the behavior of the default cluster cache impl.
	nodes := ci.data.Nodes
	cloneList := make([]*v1.Node, 0, len(nodes))
	for _, v := range nodes {
		cloneList = append(cloneList, v.DeepCopy())
	}
	return cloneList
}

// GetAllPods returns all the cached pods
func (ci *ClusterImporter) GetAllPods() []*v1.Pod {
	ci.dataLock.Lock()
	defer ci.dataLock.Unlock()

	// Deep copy here to avoid callers from corrupting the cache
	// This also mimics the behavior of the default cluster cache impl.
	pods := ci.data.Pods
	cloneList := make([]*v1.Pod, 0, len(pods))
	for _, v := range pods {
		cloneList = append(cloneList, v.DeepCopy())
	}
	return cloneList
}

// GetAllServices returns all the cached services
func (ci *ClusterImporter) GetAllServices() []*v1.Service {
	ci.dataLock.Lock()
	defer ci.dataLock.Unlock()

	// Deep copy here to avoid callers from corrupting the cache
	// This also mimics the behavior of the default cluster cache impl.
	services := ci.data.Services
	cloneList := make([]*v1.Service, 0, len(services))
	for _, v := range services {
		cloneList = append(cloneList, v.DeepCopy())
	}
	return cloneList
}

// GetAllDaemonSets returns all the cached DaemonSets
func (ci *ClusterImporter) GetAllDaemonSets() []*appsv1.DaemonSet {
	ci.dataLock.Lock()
	defer ci.dataLock.Unlock()

	// Deep copy here to avoid callers from corrupting the cache
	// This also mimics the behavior of the default cluster cache impl.
	daemonSets := ci.data.DaemonSets
	cloneList := make([]*appsv1.DaemonSet, 0, len(daemonSets))
	for _, v := range daemonSets {
		cloneList = append(cloneList, v.DeepCopy())
	}
	return cloneList
}

// GetAllDeployments returns all the cached deployments
func (ci *ClusterImporter) GetAllDeployments() []*appsv1.Deployment {
	ci.dataLock.Lock()
	defer ci.dataLock.Unlock()

	// Deep copy here to avoid callers from corrupting the cache
	// This also mimics the behavior of the default cluster cache impl.
	deployments := ci.data.Deployments
	cloneList := make([]*appsv1.Deployment, 0, len(deployments))
	for _, v := range deployments {
		cloneList = append(cloneList, v.DeepCopy())
	}
	return cloneList
}

// GetAllStatfulSets returns all the cached StatefulSets
func (ci *ClusterImporter) GetAllStatefulSets() []*appsv1.StatefulSet {
	ci.dataLock.Lock()
	defer ci.dataLock.Unlock()

	// Deep copy here to avoid callers from corrupting the cache
	// This also mimics the behavior of the default cluster cache impl.
	statefulSets := ci.data.StatefulSets
	cloneList := make([]*appsv1.StatefulSet, 0, len(statefulSets))
	for _, v := range statefulSets {
		cloneList = append(cloneList, v.DeepCopy())
	}
	return cloneList
}

// GetAllReplicaSets returns all the cached ReplicaSets
func (ci *ClusterImporter) GetAllReplicaSets() []*appsv1.ReplicaSet {
	ci.dataLock.Lock()
	defer ci.dataLock.Unlock()

	// Deep copy here to avoid callers from corrupting the cache
	// This also mimics the behavior of the default cluster cache impl.
	replicaSets := ci.data.ReplicaSets
	cloneList := make([]*appsv1.ReplicaSet, 0, len(replicaSets))
	for _, v := range replicaSets {
		cloneList = append(cloneList, v.DeepCopy())
	}
	return cloneList
}

// GetAllPersistentVolumes returns all the cached persistent volumes
func (ci *ClusterImporter) GetAllPersistentVolumes() []*v1.PersistentVolume {
	ci.dataLock.Lock()
	defer ci.dataLock.Unlock()

	// Deep copy here to avoid callers from corrupting the cache
	// This also mimics the behavior of the default cluster cache impl.
	pvs := ci.data.PersistentVolumes
	cloneList := make([]*v1.PersistentVolume, 0, len(pvs))
	for _, v := range pvs {
		cloneList = append(cloneList, v.DeepCopy())
	}
	return cloneList
}

// GetAllPersistentVolumeClaims returns all the cached persistent volume claims
func (ci *ClusterImporter) GetAllPersistentVolumeClaims() []*v1.PersistentVolumeClaim {
	ci.dataLock.Lock()
	defer ci.dataLock.Unlock()

	// Deep copy here to avoid callers from corrupting the cache
	// This also mimics the behavior of the default cluster cache impl.
	pvcs := ci.data.PersistentVolumeClaims
	cloneList := make([]*v1.PersistentVolumeClaim, 0, len(pvcs))
	for _, v := range pvcs {
		cloneList = append(cloneList, v.DeepCopy())
	}
	return cloneList
}

// GetAllStorageClasses returns all the cached storage classes
func (ci *ClusterImporter) GetAllStorageClasses() []*stv1.StorageClass {
	ci.dataLock.Lock()
	defer ci.dataLock.Unlock()

	// Deep copy here to avoid callers from corrupting the cache
	// This also mimics the behavior of the default cluster cache impl.
	storageClasses := ci.data.StorageClasses
	cloneList := make([]*stv1.StorageClass, 0, len(storageClasses))
	for _, v := range storageClasses {
		cloneList = append(cloneList, v.DeepCopy())
	}
	return cloneList
}

// GetAllJobs returns all the cached jobs
func (ci *ClusterImporter) GetAllJobs() []*batchv1.Job {
	ci.dataLock.Lock()
	defer ci.dataLock.Unlock()

	// Deep copy here to avoid callers from corrupting the cache
	// This also mimics the behavior of the default cluster cache impl.
	jobs := ci.data.Jobs
	cloneList := make([]*batchv1.Job, 0, len(jobs))
	for _, v := range jobs {
		cloneList = append(cloneList, v.DeepCopy())
	}
	return cloneList
}

// GetAllPodDisruptionBudgets returns all cached pod disruption budgets
func (ci *ClusterImporter) GetAllPodDisruptionBudgets() []*v1beta1.PodDisruptionBudget {
	ci.dataLock.Lock()
	defer ci.dataLock.Unlock()

	// Deep copy here to avoid callers from corrupting the cache
	// This also mimics the behavior of the default cluster cache impl.
	pdbs := ci.data.PodDisruptionBudgets
	cloneList := make([]*v1beta1.PodDisruptionBudget, 0, len(pdbs))
	for _, v := range pdbs {
		cloneList = append(cloneList, v.DeepCopy())
	}
	return cloneList
}

func (ci *ClusterImporter) GetAllReplicationControllers() []*v1.ReplicationController {
	ci.dataLock.Lock()
	defer ci.dataLock.Unlock()

	// Deep copy here to avoid callers from corrupting the cache
	// This also mimics the behavior of the default cluster cache impl.
	rcs := ci.data.ReplicationControllers
	cloneList := make([]*v1.ReplicationController, 0, len(rcs))
	for _, v := range rcs {
		cloneList = append(cloneList, v.DeepCopy())
	}
	return cloneList
}

// SetConfigMapUpdateFunc sets the configmap update function
func (ci *ClusterImporter) SetConfigMapUpdateFunc(_ func(interface{})) {
	// TODO: (bolt) This function is still a bit strange to me for the ClusterCache interface.
	// TODO: (bolt) no-op for now.
	log.Warnf("SetConfigMapUpdateFunc is disabled for imported cluster data.")
}
