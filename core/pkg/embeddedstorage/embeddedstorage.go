package embeddedstorage

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/opencost/opencost/core/pkg/storage"
)

// EmbeddedStorage implements storage.Storage by reading directly from embedded bytes
// without copying them to memory. This should be read-only!
type EmbeddedStorage struct {
	data []byte
	path string
}

// NewEmbeddedStorage creates a storage that serves embedded data without copying it
func NewEmbeddedStorage(data []byte, path string) *EmbeddedStorage {
	return &EmbeddedStorage{
		data: data,
		path: path,
	}
}

func (es *EmbeddedStorage) String() string {
	return string(es.StorageType())
}

func (es *EmbeddedStorage) StorageType() storage.StorageType {
	return storage.StorageTypeEmbedded
}

func (es *EmbeddedStorage) FullPath(path string) string {
	return path
}

func (es *EmbeddedStorage) Stat(path string) (*storage.StorageInfo, error) {
	if path != es.path {
		return nil, storage.DoesNotExistError
	}
	return &storage.StorageInfo{
		Name:    es.path,
		Size:    int64(len(es.data)),
		ModTime: time.Now(),
	}, nil
}

// Read returns the embedded data directly without copying
func (es *EmbeddedStorage) Read(path string) ([]byte, error) {
	if path != es.path {
		return nil, storage.DoesNotExistError
	}
	return es.data, nil
}

func (es *EmbeddedStorage) ReadStream(path string) (io.ReadCloser, error) {
	if path != es.path {
		return nil, storage.DoesNotExistError
	}
	return io.NopCloser(bytes.NewReader(es.data)), nil
}

func (es *EmbeddedStorage) ReadToLocalFile(path, destPath string) error {
	if path != es.path {
		return storage.DoesNotExistError
	}
	return os.WriteFile(destPath, es.data, 0644)
}

func (es *EmbeddedStorage) Write(path string, data []byte) error {
	return fmt.Errorf("embedded storage is read-only")
}

func (es *EmbeddedStorage) WriteStream(path string) (io.WriteCloser, error) {
	return nil, fmt.Errorf("embedded storage is read-only")
}

func (es *EmbeddedStorage) Remove(path string) error {
	return fmt.Errorf("embedded storage is read-only")
}

func (es *EmbeddedStorage) Exists(path string) (bool, error) {
	return path == es.path, nil
}

func (es *EmbeddedStorage) List(path string) ([]*storage.StorageInfo, error) {
	if path == "" || path == "." || path == "/" {
		return []*storage.StorageInfo{
			{
				Name:    es.path,
				Size:    int64(len(es.data)),
			},
		}, nil
	}
	return []*storage.StorageInfo{}, nil
}

func (es *EmbeddedStorage) ListDirectories(path string) ([]*storage.StorageInfo, error) {
	return []*storage.StorageInfo{}, nil
}
