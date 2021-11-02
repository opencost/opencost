package clusters

// MapDBClusterStorage is a map implementation of a database used to store cluster definitions
type MapDBClusterStorage struct {
	store map[string][]byte
}

// NewMapDBClusterStorage creates a new map backed ClusterStorage implementation
func NewMapDBClusterStorage() ClusterStorage {
	return &MapDBClusterStorage{
		store: make(map[string][]byte),
	}
}

// AddIfNotExists Adds the entry if the key does not exist
func (cs *MapDBClusterStorage) AddIfNotExists(key string, cluster []byte) error {
	if _, ok := cs.store[key]; !ok {
		cs.store[key] = cluster
	}
	return nil
}

// AddOrUpdate Adds the encoded cluster to storage if it doesn't exist. Otherwise, update the existing
// value with the provided.
func (cs *MapDBClusterStorage) AddOrUpdate(key string, cluster []byte) error {
	cs.store[key] = cluster
	return nil
}

// Remove Removes a key from the cluster storage
func (cs *MapDBClusterStorage) Remove(key string) error {
	delete(cs.store, key)
	return nil
}

// Each Iterates through all key/values for the storage and calls the handler func. If a handler returns
// an error, the iteration stops.
func (cs *MapDBClusterStorage) Each(handler func(string, []byte) error) error {
	for k, v := range cs.store {
		value := make([]byte, len(v))
		copy(value, v)

		if err := handler(k, value); err != nil {
			return err
		}
	}
	return nil
}

// Close Closes the backing storage
func (cs *MapDBClusterStorage) Close() error {
	return nil
}
