package memfile

import (
	"fmt"
	"path/filepath"
	"strings"
)

// SplitPaths splits the directory path into a slice of directory names.
func SplitPaths(path string) []string {
	path = filepath.Clean(path)
	if path[len(path)-1] == filepath.Separator {
		path = path[:len(path)-1]
	}
	return strings.Split(path, string(filepath.Separator))
}

// Split splits the path into a slice of directory names and the file name.
func Split(path string) ([]string, string) {
	path = filepath.Clean(path)
	pDir, pFile := filepath.Split(path)
	pDir = filepath.Dir(pDir)

	return strings.Split(pDir, string(filepath.Separator)), pFile
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
			return nil, fmt.Errorf("directory %s not found", filepath.Join(paths[:i+1]...))
		}
		currentDir = currentDir.dirs[dirName]
	}

	return currentDir, nil
}
