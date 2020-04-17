package clustermanager

import (
	"encoding/json"
	"io/ioutil"

	"github.com/google/uuid"

	"github.com/kubecost/cost-model/pkg/util"

	"k8s.io/klog"
	"sigs.k8s.io/yaml"
)

// Cluster definition from a configuration yaml
type ClusterConfigEntry struct {
	Name    string                 `yaml:"name"`
	Address string                 `yaml:"address"`
	Details map[string]interface{} `yaml:"details,omitempty"`
}

// ClusterDefinition
type ClusterDefinition struct {
	ID      string                 `json:"id,omitempty"`
	Name    string                 `json:"name"`
	Address string                 `json:"address"`
	Details map[string]interface{} `json:"details,omitempty"`
}

// ClusterStorage interface defines an implementation prototype for a storage responsible
// for ClusterDefinition instances
type ClusterStorage interface {
	// Add only if the key does not exist
	AddIfNotExists(key string, cluster []byte) error

	// Adds the encoded cluster to storage if it doesn't exist. Otherwise, update the existing
	// value with the provided.
	AddOrUpdate(key string, cluster []byte) error

	// Removes a key from the cluster storage
	Remove(key string) error

	// Iterates through all key/values for the storage and calls the handler func. If a handler returns
	// an error, the iteration stops.
	Each(handler func(string, []byte) error) error

	// Closes the backing storage
	Close() error
}

type ClusterManager struct {
	storage ClusterStorage
	// cache   map[string]*ClusterDefinition
}

// Creates a new ClusterManager instance using the provided storage
func NewClusterManager(storage ClusterStorage) *ClusterManager {
	return &ClusterManager{
		storage: storage,
	}
}

// Creates a new ClusterManager instance using the provided storage and populates a
// yaml configured list of clusters
func NewConfiguredClusterManager(storage ClusterStorage, config string) *ClusterManager {
	clusterManager := NewClusterManager(storage)

	exists, err := util.FileExists(config)
	if !exists {
		if err != nil {
			klog.V(1).Infof("[Error] Failed to load config file: %s. Error: %s", config, err.Error())
		}
		return clusterManager
	}

	data, err := ioutil.ReadFile(config)
	if err != nil {
		return clusterManager
	}

	var entries []ClusterConfigEntry
	err = yaml.Unmarshal(data, &entries)
	if err != nil {
		return clusterManager
	}

	for _, entry := range entries {
		clusterManager.Add(ClusterDefinition{
			ID:      entry.Name,
			Name:    entry.Name,
			Address: entry.Address,
			Details: entry.Details,
		})
	}

	return clusterManager
}

// Adds, but will not update an existing entry.
func (cm *ClusterManager) Add(cluster ClusterDefinition) (*ClusterDefinition, error) {
	// First time add
	if cluster.ID == "" {
		cluster.ID = uuid.New().String()
	}

	data, err := json.Marshal(cluster)
	if err != nil {
		return nil, err
	}

	err = cm.storage.AddIfNotExists(cluster.ID, data)
	if err != nil {
		return nil, err
	}

	return &cluster, nil
}

func (cm *ClusterManager) AddOrUpdate(cluster ClusterDefinition) (*ClusterDefinition, error) {
	// First time add
	if cluster.ID == "" {
		cluster.ID = uuid.New().String()
	}

	data, err := json.Marshal(cluster)
	if err != nil {
		return nil, err
	}

	err = cm.storage.AddOrUpdate(cluster.ID, data)
	if err != nil {
		return nil, err
	}

	return &cluster, nil
}

func (cm *ClusterManager) Remove(id string) error {
	return cm.storage.Remove(id)
}

func (cm *ClusterManager) GetAll() []*ClusterDefinition {
	clusters := []*ClusterDefinition{}

	err := cm.storage.Each(func(key string, cluster []byte) error {
		var cd ClusterDefinition
		err := json.Unmarshal(cluster, &cd)
		if err != nil {
			klog.V(1).Infof("[Error] Failed to unmarshal json cluster definition for key: %s", key)
			return nil
		}

		clusters = append(clusters, &cd)
		return nil
	})

	if err != nil {
		klog.Infof("[Error] Failed to load list of clusters: %s", err.Error())
	}

	return clusters
}

func (cm *ClusterManager) Close() error {
	return cm.storage.Close()
}
