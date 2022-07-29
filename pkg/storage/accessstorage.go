package storage

import (
	"github.com/opencost/opencost/pkg/env"
	"github.com/opencost/opencost/pkg/log"
)

// AccessStorage is a Storage implementation that sits on top of other Storage implementations. It controls which functions
// make it to the underlying interface based configurations allowing all storage types to share these behaviours.
type AccessStorage struct {
	storage Storage
}

// StorageType returns a string identifier for the type of storage used by the implementation.
func (as *AccessStorage) StorageType() StorageType {
	return as.storage.StorageType()
}

// FullPath returns the storage working path combined with the path provided
func (as *AccessStorage) FullPath(path string) string {
	return as.storage.FullPath(path)
}

// Stat returns the StorageStats for the specific path.
func (as *AccessStorage) Stat(path string) (*StorageInfo, error) {
	return as.storage.Stat(path)
}

// Read uses the relative path of the storage combined with the provided path to
// read the contents.
func (as *AccessStorage) Read(path string) ([]byte, error) {
	return as.storage.Read(path)
}

// Write uses the relative path of the storage combined with the provided path
// to write a new file or overwrite an existing file.
func (as *AccessStorage) Write(path string, data []byte) error {
	if env.IsETLStoreReadOnlyMode() {
		log.DedupedInfof(1, "AccessStorage: skipping storage Write")
		return nil
	}
	return as.storage.Write(path, data)
}

// Remove uses the relative path of the storage combined with the provided path to
// remove a file from storage permanently.
func (as *AccessStorage) Remove(path string) error {
	if env.IsETLStoreReadOnlyMode() {
		log.DedupedInfof(1, "AccessStorage: skipping storage Remove")
		return nil
	}
	return as.storage.Remove(path)
}

// Exists uses the relative path of the storage combined with the provided path to
// determine if the file exists.
func (as *AccessStorage) Exists(path string) (bool, error) {
	return as.storage.Exists(path)
}

// List uses the relative path of the storage combined with the provided path to return
// storage information for the files.
func (as *AccessStorage) List(path string) ([]*StorageInfo, error) {
	return as.storage.List(path)
}
