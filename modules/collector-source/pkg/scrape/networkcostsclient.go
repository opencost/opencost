package scrape

import (
	"github.com/opencost/opencost/core/pkg/clustercache"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// NetworkCostsClient provides access to Kubernetes API for NetworkCosts scraping.
// Unlike StatSummaryClient which performs data collection, this client only provides
// the Kubernetes clientset needed for K8s API proxy access. The actual scraping logic
// is handled by NetworkTargetProvider.GetTargets() which creates appropriate ScrapeTargets.
type NetworkCostsClient interface {
	// GetKubeClient returns a Kubernetes clientset for creating K8sProxyTargets.
	// Returns nil if proxy mode should be disabled (e.g., client creation failed).
	GetKubeClient() kubernetes.Interface
}

// NetworkCostsK8sClient implements NetworkCostsClient using Kubernetes API
type NetworkCostsK8sClient struct {
	cache      clustercache.ClusterCache
	kubeClient kubernetes.Interface
}

// NewNetworkCostsClient creates a new NetworkCostsClient.
// Signature matches NewNodeStatsSummaryClient pattern for consistency.
//
// Note: Unlike StatSummaryClient which does data collection, this client only provides
// the Kubernetes clientset. The scraping is delegated to the existing NetworkTargetProvider
// which already handles target discovery and scraping via the ScrapeTarget interface.
func NewNetworkCostsClient(cache clustercache.ClusterCache, config *rest.Config) NetworkCostsClient {
	kubeClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		// In production, this should not fail as config is already validated.
		// Return a client with nil kubeClient - proxy mode will be disabled and
		// NetworkTargetProvider will fall back to direct HTTP scraping.
		return &NetworkCostsK8sClient{
			cache:      cache,
			kubeClient: nil,
		}
	}

	return &NetworkCostsK8sClient{
		cache:      cache,
		kubeClient: kubeClient,
	}
}

// GetKubeClient returns the Kubernetes clientset for creating K8sProxyTargets.
// Returns nil if the client could not be created, which signals NetworkTargetProvider
// to use direct HTTP scraping instead of K8s API proxy.
func (c *NetworkCostsK8sClient) GetKubeClient() kubernetes.Interface {
	return c.kubeClient
}
