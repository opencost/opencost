package services

import (
	"path"

	"github.com/opencost/opencost/pkg/env"
	"github.com/opencost/opencost/pkg/services/clusters"
)

// NewClusterManagerService creates a new HTTPService implementation driving cluster definition management
// for the frontend
func NewClusterManagerService() HTTPService {
	return clusters.NewClusterManagerHTTPService(newClusterManager())
}

// newClusterManager creates a new cluster manager instance for use in the service
func newClusterManager() *clusters.ClusterManager {
	clustersConfigFile := path.Join(env.GetCostAnalyzerVolumeMountPath(), "clusters/default-clusters.yaml")

	// Return a memory-backed cluster manager populated by configmap
	return clusters.NewConfiguredClusterManager(clusters.NewMapDBClusterStorage(), clustersConfigFile)
}
