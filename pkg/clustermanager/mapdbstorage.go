package clustermanager

import (
	_ "k8s.io/klog"
)

type MapDBClusterStorage struct {
	store map[string][]byte
}

func NewMapDBClusterStorage() ClusterStorage {
	return &MapDBClusterStorage{
		store: make(map[string][]byte),
	}
}

// Adds the entry if the key does not exist
func (cs *MapDBClusterStorage) AddIfNotExists(key string, cluster []byte) error {
	if _, ok := cs.store[key]; !ok {
		cs.store[key] = cluster
	}
	return nil
}

// Adds the encoded cluster to storage if it doesn't exist. Otherwise, update the existing
// value with the provided.
func (cs *MapDBClusterStorage) AddOrUpdate(key string, cluster []byte) error {
	cs.store[key] = cluster
	return nil
}

// Removes a key from the cluster storage
func (cs *MapDBClusterStorage) Remove(key string) error {
	delete(cs.store, key)
	return nil
}

// Retrieve all the keys stored
func (cs *MapDBClusterStorage) Each(handler func(string, []byte)) error {
	for k, v := range cs.store {
		value := make([]byte, len(v))
		copy(value, v)

		handler(k, value)
	}
	return nil
}

// Closes the backing storage
func (cs *MapDBClusterStorage) Close() error {
	return nil
}
