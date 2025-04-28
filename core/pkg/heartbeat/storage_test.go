package heartbeat

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/storage"
)

type file struct {
	name     string
	contents []byte
}

func newFile(name string, contents []byte) *file {
	return &file{
		name:     name,
		contents: contents,
	}
}

type dir struct {
	name  string
	dirs  map[string]*dir
	files map[string]*file
}

func newDir(name string) *dir {
	return &dir{
		name:  name,
		dirs:  make(map[string]*dir),
		files: make(map[string]*file),
	}
}

func (d *dir) size() int64 {
	var size int64
	for _, f := range d.files {
		size += int64(len(f.contents))
	}
	for _, subdir := range d.dirs {
		size += subdir.size()
	}
	return size
}

func (d *dir) addFile(f *file) {
	d.files[f.name] = f
}

func (d *dir) addDir(subdir *dir) {
	d.dirs[subdir.name] = subdir
}

func (d *dir) deleteFile(name string) {
	delete(d.files, name)
}

func (d *dir) deleteDir(name string) {
	delete(d.dirs, name)
}

type MockStorage struct {
	s map[string][]byte
	d *dir
}

func NewMockStorage() *MockStorage {
	return &MockStorage{
		s: make(map[string][]byte),
		d: newDir(""),
	}
}

// StorageType returns a string identifier for the type of storage used by the implementation.
func (ms *MockStorage) StorageType() storage.StorageType {
	return storage.StorageType("mock")
}

// FullPath returns the storage working path combined with the path provided
func (ms *MockStorage) FullPath(path string) string {
	return path
}

// Stat returns the StorageStats for the specific path.
func (ms *MockStorage) Stat(path string) (*storage.StorageInfo, error) {
	path = filepath.Clean(path)
	if data, ok := ms.s[path]; ok {
		return &storage.StorageInfo{
			Name:    path,
			Size:    int64(len(data)),
			ModTime: time.Now(),
		}, nil
	}

	return nil, fmt.Errorf("no valid data found")
}

// Read uses the relative path of the storage combined with the provided path to
// read the contents.
func (ms *MockStorage) Read(path string) ([]byte, error) {
	path = filepath.Clean(path)

	if data, ok := ms.s[path]; ok {
		return data, nil
	}

	return nil, fmt.Errorf("no valid data found")
}

// Write uses the relative path of the storage combined with the provided path
// to write a new file or overwrite an existing file.
func (ms *MockStorage) Write(path string, data []byte) error {
	paths, pFile := pathsAndFile(path)

	f := newFile(pFile, data)
	currentDir := writeDir(ms.d, paths)

	currentDir.addFile(f)
	ms.s[path] = data
	return nil
}

// Remove uses the relative path of the storage combined with the provided path to
// remove a file from storage permanently.
func (ms *MockStorage) Remove(path string) error {
	paths, pFile := pathsAndFile(path)

	currentDir, err := searchDir(ms.d, paths)
	if err != nil {
		return err
	}

	currentDir.deleteFile(pFile)

	delete(ms.s, path)
	return nil
}

// Exists uses the relative path of the storage combined with the provided path to
// determine if the file exists.
func (ms *MockStorage) Exists(path string) (bool, error) {
	path = filepath.Clean(path)

	_, ok := ms.s[path]
	return ok, nil
}

// List uses the relative path of the storage combined with the provided path to return
// storage information for the files.
func (ms *MockStorage) List(path string) ([]*storage.StorageInfo, error) {
	paths := toPaths(path)
	currentDir, err := searchDir(ms.d, paths)
	if err != nil {
		return nil, err
	}

	storageInfos := make([]*storage.StorageInfo, 0, len(currentDir.files))
	for _, f := range currentDir.files {
		storageInfos = append(storageInfos, &storage.StorageInfo{
			Name:    f.name,
			Size:    int64(len(f.contents)),
			ModTime: time.Now(),
		})
	}

	return storageInfos, nil
}

// ListDirectories uses the relative path of the storage combined with the provided path
// to return storage information for only directories contained along the path. This
// functions as List, but returns storage information for only directories.
func (ms *MockStorage) ListDirectories(path string) ([]*storage.StorageInfo, error) {
	paths := toPaths(path)
	currentDir, err := searchDir(ms.d, paths)
	if err != nil {
		return nil, err
	}

	storageInfos := make([]*storage.StorageInfo, 0, len(currentDir.files))
	for _, d := range currentDir.dirs {
		storageInfos = append(storageInfos, &storage.StorageInfo{
			Name:    d.name,
			Size:    d.size(),
			ModTime: time.Now(),
		})
	}

	return storageInfos, nil
}

func toPaths(path string) []string {
	path = filepath.Clean(path)
	if path[len(path)-1] == filepath.Separator {
		path = path[:len(path)-1]
	}
	return strings.Split(path, string(filepath.Separator))
}

func pathsAndFile(path string) ([]string, string) {
	path = filepath.Clean(path)
	pDir, pFile := filepath.Split(path)
	pDir = filepath.Dir(pDir)
	return strings.Split(pDir, string(filepath.Separator)), pFile
}

func writeDir(d *dir, paths []string) *dir {
	currentDir := d

	for i := 0; i < len(paths); i++ {
		dirName := paths[i]
		if _, ok := currentDir.dirs[dirName]; !ok {
			currentDir.addDir(newDir(dirName))
		}
		currentDir = currentDir.dirs[dirName]
	}

	return currentDir
}

func searchDir(d *dir, paths []string) (*dir, error) {
	currentDir := d

	for i := 0; i < len(paths); i++ {
		dirName := paths[i]
		if _, ok := currentDir.dirs[dirName]; !ok {
			return nil, fmt.Errorf("directory %s not found", filepath.Join(paths[:i+1]...))
		}
		currentDir = currentDir.dirs[dirName]
	}

	return currentDir, nil
}
