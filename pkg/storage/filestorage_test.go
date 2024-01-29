package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFileStorageListDirectoriesSymlink(t *testing.T) {
	storageBaseDir := t.TempDir()
	subdirName := "sub"
	slFileName := "sl"
	symlinkTargetDir := t.TempDir()

	if err := os.MkdirAll(filepath.Join(storageBaseDir, subdirName), 0700); err != nil {
		t.Fatalf("failed to construct subdir: %s", err)
	}

	if err := os.Symlink(symlinkTargetDir, filepath.Join(storageBaseDir, subdirName, slFileName)); err != nil {
		t.Fatalf("failed to create symlink at '%s' which points to '%s': %s", filepath.Join(storageBaseDir, subdirName, slFileName), symlinkTargetDir, err)
	}

	store := FileStorage{baseDir: storageBaseDir}
	dirs, err := store.ListDirectories(subdirName)
	if err != nil {
		t.Fatalf("failed to list directories: %s", err)
	}
	if len(dirs) != 1 {
		t.Fatalf("dirs should have 1 entry")
	}
	dir := dirs[0]
	// This condition is important -- the returned path should be relative to
	// the storage base dir, not the symlink's target dir.
	if dir.Name != filepath.Join(subdirName, slFileName) {
		t.Errorf("Expected dir.Name to be '%s' but it was '%s'", filepath.Join(subdirName, slFileName), dir.Name)
	}
}
