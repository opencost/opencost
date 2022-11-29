package storage

import (
	gofs "io/fs"
	"os"
	gopath "path"
	"path/filepath"

	"github.com/opencost/opencost/pkg/util/fileutil"
	"github.com/pkg/errors"
)

// FileStorage leverages the file system to write data to disk.
type FileStorage struct {
	baseDir string
}

// NewFileStorage returns a new storage API which leverages the file system.
func NewFileStorage(baseDir string) Storage {
	return &FileStorage{baseDir}
}

// StorageType returns a string identifier for the type of storage used by the implementation.
func (fs *FileStorage) StorageType() StorageType {
	return StorageTypeFile
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

		return nil, errors.Wrap(err, "Failed to stat file")
	}

	return FileToStorageInfo(st), nil
}

// List uses the relative path of the storage combined with the provided path to return
// storage information for the files.
func (fs *FileStorage) List(path string) ([]*StorageInfo, error) {
	p := gopath.Join(fs.baseDir, path)

	// Read files in the backup path
	entries, err := os.ReadDir(p)
	if err != nil {
		return nil, err
	}
	files := make([]gofs.FileInfo, 0, len(entries))
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		files = append(files, info)
	}

	return FilesToStorageInfo(files), nil
}

func (fs *FileStorage) ListDirectories(path string) ([]*StorageInfo, error) {
	p := gopath.Join(fs.baseDir, path)

	// Read files in the backup path
	entries, err := os.ReadDir(p)
	if err != nil {
		return nil, err
	}
	files := make([]gofs.FileInfo, 0, len(entries))
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		files = append(files, info)
	}

	return DirFilesToStorageInfo(files, path), nil
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
		return nil, errors.Wrap(err, "Failed to read file")
	}

	return b, nil
}

// Write uses the relative path of the storage combined with the provided path
// to write a new file or overwrite an existing file.
func (fs *FileStorage) Write(path string, data []byte) error {
	f, err := fs.prepare(path)
	if err != nil {
		return errors.Wrap(err, "Failed to prepare path")
	}
	err = os.WriteFile(f, data, os.ModePerm)
	if err != nil {
		return errors.Wrap(err, "Failed to write file")
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

		return errors.Wrap(err, "Failed to remove file")
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

// FilesToStorageInfo maps a []fs.FileInfo to []*storage.StorageInfo
func FilesToStorageInfo(fileInfo []gofs.FileInfo) []*StorageInfo {
	var stats []*StorageInfo
	for _, info := range fileInfo {
		stats = append(stats, FileToStorageInfo(info))
	}
	return stats
}

// FileToStorageInfo maps a fs.FileInfo to *storage.StorageInfo
func FileToStorageInfo(fileInfo gofs.FileInfo) *StorageInfo {
	return &StorageInfo{
		Name:    fileInfo.Name(),
		Size:    fileInfo.Size(),
		ModTime: fileInfo.ModTime(),
	}
}

// DirFilesToStorageInfo maps a []fs.FileInfo to []*storage.StorageInfo
// but only returning StorageInfo for directories
func DirFilesToStorageInfo(fileInfo []gofs.FileInfo, path string) []*StorageInfo {
	var stats []*StorageInfo
	for _, info := range fileInfo {
		if info.IsDir() {
			stats = append(stats, &StorageInfo{
				Name:    filepath.Join(path, info.Name()),
				Size:    info.Size(),
				ModTime: info.ModTime(),
			})
		}
	}
	return stats
}
