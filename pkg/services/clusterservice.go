package services

import "github.com/kubecost/cost-model/pkg/services/clusters"

// NewClusterManagerService creates a new HTTPService implementation driving cluster definition management
// for the frontend
func NewClusterManagerService() HTTPService {
	return clusters.NewClusterManagerHTTPService(newClusterManager())
}

// newClusterManager creates a new cluster manager instance for use in the service
func newClusterManager() *clusters.ClusterManager {
	clustersConfigFile := "/var/configs/clusters/default-clusters.yaml"

	// Return a memory-backed cluster manager populated by configmap
	return clusters.NewConfiguredClusterManager(clusters.NewMapDBClusterStorage(), clustersConfigFile)

	// NOTE: The following should be used with a persistent disk store. Since the
	// NOTE: configmap approach is currently the "persistent" source (entries are read-only
	// NOTE: on the backend), we don't currently need to store on disk.
	/*
		path := env.GetConfigPath()
		db, err := bolt.Open(path+"costmodel.db", 0600, nil)
		if err != nil {
			log.Errorf("[Error] Failed to create costmodel.db: %s", err.Error())
			return cm.NewConfiguredClusterManager(cm.NewMapDBClusterStorage(), clustersConfigFile)
		}

		store, err := clusters.NewBoltDBClusterStorage("clusters", db)
		if err != nil {
			log.Errorf("[Error] Failed to Create Cluster Storage: %s", err.Error())
			return clusters.NewConfiguredClusterManager(clusters.NewMapDBClusterStorage(), clustersConfigFile)
		}

		return clusters.NewConfiguredClusterManager(store, clustersConfigFile)
	*/
}
