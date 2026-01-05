package storage

import (
	"fmt"
	"path/filepath"
	"sync"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/storage/memfile"
)

// MemoryStorage is a thread-safe in-memory file system storage implementation. It can be used for testing storage.Storage dependents
// or to serve as a lightweight storage implementation within a production system.
type MemoryStorage struct {
	lock        sync.Mutex
	directPaths map[string]*memfile.MemoryFile
	fileTree    *memfile.MemoryDirectory
}

// NewMemoryStorage creates a new in-memory file system storage implementation.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		directPaths: make(map[string]*memfile.MemoryFile),
		fileTree:    memfile.NewMemoryDirectory(""),
	}
}

// String returns the storage type as a string for logging purposes.
func (ms *MemoryStorage) String() string {
	return string(ms.StorageType())
}

// StorageType returns a string identifier for the type of storage used by the implementation.
func (ms *MemoryStorage) StorageType() StorageType {
	return StorageTypeMemory
}

// FullPath returns the storage working path combined with the path provided
func (ms *MemoryStorage) FullPath(path string) string {
	return path
}

// Stat returns the StorageStats for the specific path.
func (ms *MemoryStorage) Stat(path string) (*StorageInfo, error) {
	ms.lock.Lock()
	defer ms.lock.Unlock()

	path = filepath.Clean(path)
	if file, ok := ms.directPaths[path]; ok {
		return &StorageInfo{
			Name:    file.Name,
			Size:    file.Size(),
			ModTime: file.ModTime,
		}, nil
	}

	return nil, fmt.Errorf("file not found: %s - %w", path, DoesNotExistError)
}

// Read uses the relative path of the storage combined with the provided path to
// read the contents.
func (ms *MemoryStorage) Read(path string) ([]byte, error) {
	ms.lock.Lock()
	defer ms.lock.Unlock()

	path = filepath.Clean(path)

	if file, ok := ms.directPaths[path]; ok {
		return file.Contents, nil
	}

	return nil, fmt.Errorf("file not found: %s - %w", path, DoesNotExistError)
}

// Write uses the relative path of the storage combined with the provided path
// to write a new file or overwrite an existing file.
func (ms *MemoryStorage) Write(path string, data []byte) error {
	ms.lock.Lock()
	defer ms.lock.Unlock()

	paths, pFile := memfile.Split(path)

	f := memfile.NewMemoryFile(pFile, data)
	currentDir := memfile.CreateSubdirectory(ms.fileTree, paths)

	currentDir.AddFile(f)
	ms.directPaths[path] = f
	return nil
}

// Remove uses the relative path of the storage combined with the provided path to
// remove a file from storage permanently.
func (ms *MemoryStorage) Remove(path string) error {
	ms.lock.Lock()
	defer ms.lock.Unlock()

	path = filepath.Clean(path)
	paths, pFile := memfile.Split(path)

	currentDir, err := memfile.FindSubdirectory(ms.fileTree, paths)
	if err != nil {
		return fmt.Errorf("file not found: %s - %w", path, DoesNotExistError)
	}

	currentDir.RemoveFile(pFile)

	delete(ms.directPaths, path)
	return nil
}

// Exists uses the relative path of the storage combined with the provided path to
// determine if the file exists.
func (ms *MemoryStorage) Exists(path string) (bool, error) {
	ms.lock.Lock()
	defer ms.lock.Unlock()

	path = filepath.Clean(path)

	_, ok := ms.directPaths[path]
	return ok, nil
}

// List uses the relative path of the storage combined with the provided path to return
// storage information for the files.
func (ms *MemoryStorage) List(path string) ([]*StorageInfo, error) {
	ms.lock.Lock()
	defer ms.lock.Unlock()

	paths := memfile.SplitPaths(path)
	currentDir, err := memfile.FindSubdirectory(ms.fileTree, paths)
	if err != nil {
		// contract for bucket storages returns an empty list in this case
		// so just log a warning, and return an empty list
		log.Warnf("failed to resolve path: %s - %s", path, err)
		return []*StorageInfo{}, nil
	}

	storageInfos := make([]*StorageInfo, 0, currentDir.FileCount())
	for f := range currentDir.Files() {
		storageInfos = append(storageInfos, &StorageInfo{
			Name:    f.Name,
			Size:    f.Size(),
			ModTime: f.ModTime,
		})
	}

	return storageInfos, nil
}

// ListDirectories uses the relative path of the storage combined with the provided path
// to return storage information for only directories contained along the path. This
// functions as List, but returns storage information for only directories.
func (ms *MemoryStorage) ListDirectories(path string) ([]*StorageInfo, error) {
	ms.lock.Lock()
	defer ms.lock.Unlock()

	paths := memfile.SplitPaths(path)
	currentDir, err := memfile.FindSubdirectory(ms.fileTree, paths)
	if err != nil {
		// contract for bucket storages returns an empty list in this case
		// so just log a warning, and return an empty list
		log.Warnf("failed to resolve path: %s - %s", path, err)
		return []*StorageInfo{}, nil
	}

	storageInfos := make([]*StorageInfo, 0, currentDir.DirCount())
	for d := range currentDir.Directories() {
		storageInfos = append(storageInfos, &StorageInfo{
			Name:    filepath.Join(append(paths, d.Name)...) + "/",
			Size:    d.Size(),
			ModTime: d.ModTime,
		})
	}

	return storageInfos, nil
}
