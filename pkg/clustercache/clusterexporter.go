package clustercache

import (
	"time"

	"github.com/opencost/opencost/pkg/config"
	"github.com/opencost/opencost/pkg/log"
	"github.com/opencost/opencost/pkg/util/atomic"
	"github.com/opencost/opencost/pkg/util/json"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/api/policy/v1beta1"
	stv1 "k8s.io/api/storage/v1"
)

// clusterEncoding is used to represent the cluster objects in the encoded states.
type clusterEncoding struct {
	Namespaces             []*v1.Namespace                `json:"namespaces,omitempty"`
	Nodes                  []*v1.Node                     `json:"nodes,omitempty"`
	Pods                   []*v1.Pod                      `json:"pods,omitempty"`
	Services               []*v1.Service                  `json:"services,omitempty"`
	DaemonSets             []*appsv1.DaemonSet            `json:"daemonSets,omitempty"`
	Deployments            []*appsv1.Deployment           `json:"deployments,omitempty"`
	StatefulSets           []*appsv1.StatefulSet          `json:"statefulSets,omitempty"`
	ReplicaSets            []*appsv1.ReplicaSet           `json:"replicaSets,omitempty"`
	PersistentVolumes      []*v1.PersistentVolume         `json:"persistentVolumes,omitempty"`
	PersistentVolumeClaims []*v1.PersistentVolumeClaim    `json:"persistentVolumeClaims,omitempty"`
	StorageClasses         []*stv1.StorageClass           `json:"storageClasses,omitempty"`
	Jobs                   []*batchv1.Job                 `json:"jobs,omitempty"`
	PodDisruptionBudgets   []*v1beta1.PodDisruptionBudget `json:"podDisruptionBudgets,omitempty"`
	ReplicationControllers []*v1.ReplicationController    `json:"replicationController,omitempty"`
}

// ClusterExporter manages and runs an file export process which dumps the local kubernetes cluster to a target location.
type ClusterExporter struct {
	cluster  ClusterCache
	target   *config.ConfigFile
	interval time.Duration
	runState atomic.AtomicRunState
}

// NewClusterExporter creates a new ClusterExporter instance for exporting the kubernetes cluster.
func NewClusterExporter(cluster ClusterCache, target *config.ConfigFile, interval time.Duration) *ClusterExporter {
	return &ClusterExporter{
		cluster:  cluster,
		target:   target,
		interval: interval,
	}
}

// Run starts the automated process of running Export on a specific interval.
func (ce *ClusterExporter) Run() {
	// in the event there is a race that occurs between Run() and Stop(), we
	// ensure that we wait for the reset to occur before starting again
	ce.runState.WaitForReset()

	if !ce.runState.Start() {
		log.Warnf("ClusterExporter already running")
		return
	}

	go func() {
		for {
			err := ce.Export()
			if err != nil {
				log.Warnf("Failed to export cluster: %s", err)
			}

			select {
			case <-time.After(ce.interval):
			case <-ce.runState.OnStop():
				ce.runState.Reset()
				return
			}
		}
	}()
}

// Stop halts the Cluster export on an interval
func (ce *ClusterExporter) Stop() {
	ce.runState.Stop()
}

// Export stores the cluster cache data into a PODO, marshals as JSON, and saves it to the
// target location.
func (ce *ClusterExporter) Export() error {
	c := ce.cluster
	encoding := &clusterEncoding{
		Namespaces:             c.GetAllNamespaces(),
		Nodes:                  c.GetAllNodes(),
		Pods:                   c.GetAllPods(),
		Services:               c.GetAllServices(),
		DaemonSets:             c.GetAllDaemonSets(),
		Deployments:            c.GetAllDeployments(),
		StatefulSets:           c.GetAllStatefulSets(),
		ReplicaSets:            c.GetAllReplicaSets(),
		PersistentVolumes:      c.GetAllPersistentVolumes(),
		PersistentVolumeClaims: c.GetAllPersistentVolumeClaims(),
		StorageClasses:         c.GetAllStorageClasses(),
		Jobs:                   c.GetAllJobs(),
		PodDisruptionBudgets:   c.GetAllPodDisruptionBudgets(),
		ReplicationControllers: c.GetAllReplicationControllers(),
	}

	data, err := json.Marshal(encoding)
	if err != nil {
		return err
	}

	return ce.target.Write(data)
}
