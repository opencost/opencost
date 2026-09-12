package memfile

import (
	"fmt"
	"path"
	"path/filepath"
	"strings"
)

// Separator is the separator used within memory storage paths. Memory storage paths are logical
// keys, like the object keys used by the bucket storages, so they are separated by '/' on every
// platform rather than by the host file system separator.
const Separator = "/"

// Normalize returns the cleaned, slash separated form of the provided path. It is the canonical
// form used to key a file within memory storage, so every path entering memory storage must pass
// through it.
func Normalize(p string) string {
	return path.Clean(filepath.ToSlash(p))
}

// Join combines the provided path elements into a single memory storage path.
func Join(elem ...string) string {
	return path.Join(elem...)
}

// SplitPaths splits the directory path into a slice of directory names.
func SplitPaths(p string) []string {
	p = Normalize(p)
	p = strings.TrimSuffix(p, Separator)

	return strings.Split(p, Separator)
}

// Split splits the path into a slice of directory names and the file name.
func Split(p string) ([]string, string) {
	p = Normalize(p)
	pDir, pFile := path.Split(p)
	pDir = path.Dir(pDir)

	return strings.Split(pDir, Separator), pFile
}

// CreateSubdirectory creates the necessary subdirectories within the provided MemoryDirectory.
func CreateSubdirectory(d *MemoryDirectory, paths []string) *MemoryDirectory {
	currentDir := d

	for i := 0; i < len(paths); i++ {
		dirName := paths[i]
		if _, ok := currentDir.dirs[dirName]; !ok {
			currentDir.AddDirectory(NewMemoryDirectory(dirName))
		}
		currentDir = currentDir.dirs[dirName]
	}

	return currentDir
}

// FindSubdirectory searches through the provided path slice starting with the provided directory,
// and returns the correct MemoryDirectory if it exists. If the directory does not exist, an error is
// returned containing the path where the find failed.
func FindSubdirectory(d *MemoryDirectory, paths []string) (*MemoryDirectory, error) {
	currentDir := d

	for i := 0; i < len(paths); i++ {
		dirName := paths[i]
		if _, ok := currentDir.dirs[dirName]; !ok {
			return nil, fmt.Errorf("directory %s not found", Join(paths[:i+1]...))
		}
		currentDir = currentDir.dirs[dirName]
	}

	return currentDir, nil
}
