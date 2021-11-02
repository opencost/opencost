package clusters

import (
	bolt "go.etcd.io/bbolt"
)

// BoltDBClusterStorage is a boltdb implementation of a database used to store cluster definitions
type BoltDBClusterStorage struct {
	bucket []byte
	db     *bolt.DB
}

// NewBoltDBClusterStorage creates a new boltdb backed ClusterStorage implementation
func NewBoltDBClusterStorage(bucket string, db *bolt.DB) (ClusterStorage, error) {
	bucketKey := []byte(bucket)

	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketKey)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &BoltDBClusterStorage{
		bucket: bucketKey,
		db:     db,
	}, nil
}

// AddIfNotExists Adds the entry if the key does not exist
func (cs *BoltDBClusterStorage) AddIfNotExists(key string, cluster []byte) error {
	return cs.db.Update(func(tx *bolt.Tx) error {
		k := []byte(key)
		bucket := tx.Bucket(cs.bucket)

		if bucket.Get(k) != nil {
			return nil
		}
		return bucket.Put(k, cluster)
	})
}

// AddOrUpdate Adds the encoded cluster to storage if it doesn't exist. Otherwise, update the existing
// value with the provided.
func (cs *BoltDBClusterStorage) AddOrUpdate(key string, cluster []byte) error {
	return cs.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(cs.bucket)

		return bucket.Put([]byte(key), cluster)
	})
}

// Remove Removes a key from the cluster storage
func (cs *BoltDBClusterStorage) Remove(key string) error {
	return cs.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(cs.bucket)

		return bucket.Delete([]byte(key))
	})
}

// Each Iterates through all key/values for the storage and calls the handler func. If a handler returns
// an error, the iteration stops.
func (cs *BoltDBClusterStorage) Each(handler func(string, []byte) error) error {
	return cs.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(cs.bucket)

		return bucket.ForEach(func(k, v []byte) error {
			// Allow the bytes to live outside transaction by copy
			key := make([]byte, len(k))
			value := make([]byte, len(v))

			copy(key, k)
			copy(value, v)

			if err := handler(string(key), value); err != nil {
				return err
			}

			return nil
		})
	})
}

// Close Closes the backing storage
func (cs *BoltDBClusterStorage) Close() error {
	return cs.db.Close()
}
