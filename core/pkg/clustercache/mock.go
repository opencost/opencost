package clustercache

// MockClusterCache is a configurable implementation of ClusterCache for use in tests.
// Each field corresponds to the slice returned by the matching GetAll* method.
// Any field left nil causes its method to return nil.
type MockClusterCache struct {
	Nodes                  []*Node
	Pods                   []*Pod
	Namespaces             []*Namespace
	Services               []*Service
	DaemonSets             []*DaemonSet
	Deployments            []*Deployment
	StatefulSets           []*StatefulSet
	ReplicaSets            []*ReplicaSet
	PersistentVolumes      []*PersistentVolume
	PersistentVolumeClaims []*PersistentVolumeClaim
	StorageClasses         []*StorageClass
	Jobs                   []*Job
	CronJobs               []*CronJob
	PodDisruptionBudgets   []*PodDisruptionBudget
	ReplicationControllers []*ReplicationController
	ResourceQuotas         []*ResourceQuota
}

func (m *MockClusterCache) Run()  {}
func (m *MockClusterCache) Stop() {}

func (m *MockClusterCache) GetAllNodes() []*Node                           { return m.Nodes }
func (m *MockClusterCache) GetAllPods() []*Pod                             { return m.Pods }
func (m *MockClusterCache) GetAllNamespaces() []*Namespace                 { return m.Namespaces }
func (m *MockClusterCache) GetAllServices() []*Service                     { return m.Services }
func (m *MockClusterCache) GetAllDaemonSets() []*DaemonSet                 { return m.DaemonSets }
func (m *MockClusterCache) GetAllDeployments() []*Deployment               { return m.Deployments }
func (m *MockClusterCache) GetAllStatefulSets() []*StatefulSet             { return m.StatefulSets }
func (m *MockClusterCache) GetAllReplicaSets() []*ReplicaSet               { return m.ReplicaSets }
func (m *MockClusterCache) GetAllPersistentVolumes() []*PersistentVolume   { return m.PersistentVolumes }
func (m *MockClusterCache) GetAllPersistentVolumeClaims() []*PersistentVolumeClaim {
	return m.PersistentVolumeClaims
}
func (m *MockClusterCache) GetAllStorageClasses() []*StorageClass { return m.StorageClasses }
func (m *MockClusterCache) GetAllJobs() []*Job                    { return m.Jobs }
func (m *MockClusterCache) GetAllCronJobs() []*CronJob            { return m.CronJobs }
func (m *MockClusterCache) GetAllPodDisruptionBudgets() []*PodDisruptionBudget {
	return m.PodDisruptionBudgets
}
func (m *MockClusterCache) GetAllReplicationControllers() []*ReplicationController {
	return m.ReplicationControllers
}
func (m *MockClusterCache) GetAllResourceQuotas() []*ResourceQuota { return m.ResourceQuotas }