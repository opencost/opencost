package storage

import (
	"fmt"
	gofs "io/fs"
	"os"
	gopath "path"
	"path/filepath"

	"github.com/opencost/opencost/pkg/util/fileutil"
)

// FileStorage leverages the file system to write data to disk.
type FileStorage struct {
	baseDir string
}

// NewFileStorage returns a new storage API which leverages the file system.
func NewFileStorage(baseDir string) *FileStorage {
	return &FileStorage{baseDir}
}

// FullPath returns the storage working path combined with the path provided
func (fs *FileStorage) FullPath(path string) string {
	return gopath.Join(fs.baseDir, path)
}

// Stat returns the StorageStats for the specific path.
func (fs *FileStorage) Stat(path string) (*StorageInfo, error) {
	f := gopath.Join(fs.baseDir, path)
	st, err := os.Stat(f)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, DoesNotExistError
		}

		return nil, fmt.Errorf("Failed to stat file: %w", err)
	}

	return FileToStorageInfo(st), nil
}

// Read uses the relative path of the storage combined with the provided path to
// read the contents.
func (fs *FileStorage) Read(path string) ([]byte, error) {
	f := gopath.Join(fs.baseDir, path)

	b, err := os.ReadFile(f)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, DoesNotExistError
		}
		return nil, fmt.Errorf("Failed to read file: %w", err)
	}

	return b, nil
}

// Write uses the relative path of the storage combined with the provided path
// to write a new file or overwrite an existing file.
func (fs *FileStorage) Write(path string, data []byte) error {
	f, err := fs.prepare(path)
	if err != nil {
		return fmt.Errorf("Failed to prepare path: %w", err)
	}
	err = os.WriteFile(f, data, os.ModePerm)
	if err != nil {
		return fmt.Errorf("Failed to write file: %w", err)
	}

	return nil
}

// Remove uses the relative path of the storage combined with the provided path to
// remove a file from storage permanently.
func (fs *FileStorage) Remove(path string) error {
	f := gopath.Join(fs.baseDir, path)

	err := os.Remove(f)
	if err != nil {
		if os.IsNotExist(err) {
			return DoesNotExistError
		}

		return fmt.Errorf("Failed to remove file: %w", err)
	}

	return nil
}

// Exists uses the relative path of the storage combined with the provided path to
// determine if the file exists.
func (fs *FileStorage) Exists(path string) (bool, error) {
	f := gopath.Join(fs.baseDir, path)
	return fileutil.FileExists(f)
}

// prepare checks to see if the directory being written to should be created before writing
// the file, and then returns the correct full path.
func (fs *FileStorage) prepare(path string) (string, error) {
	f := gopath.Join(fs.baseDir, path)
	dir := filepath.Dir(f)
	if _, e := os.Stat(dir); e != nil && os.IsNotExist(e) {
		err := os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			return "", err
		}
	}

	return f, nil
}

// FileToStorageInfo maps a fs.FileInfo to *storage.StorageInfo
func FileToStorageInfo(fileInfo gofs.FileInfo) *StorageInfo {
	return &StorageInfo{
		Name:    fileInfo.Name(),
		Size:    fileInfo.Size(),
		ModTime: fileInfo.ModTime(),
	}
}
