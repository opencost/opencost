package nodestats

import (
	"context"
	"crypto/tls"
	"net/http"
	"testing"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/kubeconfig"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

func TestNodeSummaryLive(t *testing.T) {
	// this requires a live kubernetes cluster, and is used to test live functionality
	// we can comment the skip if we integrate a k8s sim or a mock server in the future
	t.Skip("Skipping live test for node summary client")

	client, err := kubeconfig.LoadKubeClient("")
	if err != nil {
		t.Fatalf("failed to load kube client: %v", err)
	}

	clusterConfig, err := kubeconfig.LoadKubeconfig("")
	if err != nil {
		t.Fatalf("failed to load kubeconfig: %v", err)
	}

	cache := NewTestClusterCache(client)

	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
			MinVersion:         tls.VersionTLS12,
		},
	}

	config := NewNodeClientConfig("cluster-one", 10, transport, "", "", NodeClientProxyConfig{
		ForceKubeProxy: true,
		LocalProxy:     "http://localhost:8080",
	})

	statsClient := NewNodeStatsSummaryClient(cache, config, clusterConfig)

	summary, err := statsClient.GetNodeData()
	if err != nil {
		t.Fatalf("failed to get node data: %v", err)
	}

	for _, s := range summary {
		if s == nil {
			t.Error("received nil summary data")
			continue
		}
		t.Logf("Node Summary: %+v", s)
	}
}

type NodesOnlyClusterCache struct {
	k8sClient kubernetes.Interface
}

func NewTestClusterCache(k8sClient kubernetes.Interface) *NodesOnlyClusterCache {
	return &NodesOnlyClusterCache{
		k8sClient: k8sClient,
	}
}

// Run starts the watcher processes
func (tcc *NodesOnlyClusterCache) Run() {}

// Stops the watcher processes
func (tcc *NodesOnlyClusterCache) Stop() {}

// GetAllNamespaces returns all the cached namespaces
func (tcc *NodesOnlyClusterCache) GetAllNamespaces() []*clustercache.Namespace { return nil }

// GetAllNodes returns all the cached nodes
func (tcc *NodesOnlyClusterCache) GetAllNodes() []*clustercache.Node {
	nodes, err := tcc.k8sClient.CoreV1().Nodes().List(context.Background(), v1.ListOptions{})
	if err != nil {
		return nil
	}

	var nodeList []*clustercache.Node
	for _, n := range nodes.Items {
		nodeList = append(nodeList, clustercache.TransformNode(&n))
	}
	return nodeList
}

// GetAllPods returns all the cached pods
func (tcc *NodesOnlyClusterCache) GetAllPods() []*clustercache.Pod { return nil }

// GetAllServices returns all the cached services
func (tcc *NodesOnlyClusterCache) GetAllServices() []*clustercache.Service { return nil }

// GetAllDaemonSets returns all the cached DaemonSets
func (tcc *NodesOnlyClusterCache) GetAllDaemonSets() []*clustercache.DaemonSet { return nil }

// GetAllDeployments returns all the cached deployments
func (tcc *NodesOnlyClusterCache) GetAllDeployments() []*clustercache.Deployment { return nil }

// GetAllStatfulSets returns all the cached StatefulSets
func (tcc *NodesOnlyClusterCache) GetAllStatefulSets() []*clustercache.StatefulSet { return nil }

// GetAllReplicaSets returns all the cached ReplicaSets
func (tcc *NodesOnlyClusterCache) GetAllReplicaSets() []*clustercache.ReplicaSet { return nil }

// GetAllPersistentVolumes returns all the cached persistent volumes
func (tcc *NodesOnlyClusterCache) GetAllPersistentVolumes() []*clustercache.PersistentVolume {
	return nil
}

// GetAllPersistentVolumeClaims returns all the cached persistent volume claims
func (tcc *NodesOnlyClusterCache) GetAllPersistentVolumeClaims() []*clustercache.PersistentVolumeClaim {
	return nil
}

// GetAllStorageClasses returns all the cached storage classes
func (tcc *NodesOnlyClusterCache) GetAllStorageClasses() []*clustercache.StorageClass { return nil }

// GetAllJobs returns all the cached jobs
func (tcc *NodesOnlyClusterCache) GetAllJobs() []*clustercache.Job { return nil }

// GetAllPodDisruptionBudgets returns all cached pod disruption budgets
func (tcc *NodesOnlyClusterCache) GetAllPodDisruptionBudgets() []*clustercache.PodDisruptionBudget {
	return nil
}

// GetAllReplicationControllers returns all cached replication controllers
func (tcc *NodesOnlyClusterCache) GetAllReplicationControllers() []*clustercache.ReplicationController {
	return nil
}

func (tcc *NodesOnlyClusterCache) GetAllResourceQuotas() []*clustercache.ResourceQuota {
	return nil
}
