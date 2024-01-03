package fileutil

import (
	"path/filepath"
	"testing"
)

// Does not test concurrency, just makes sure the basic read write functionality
// works
func TestRWLocked(t *testing.T) {
	toWrite := "hello world"
	dir := t.TempDir()
	filename := filepath.Join(dir, "test.txt")

	if _, err := WriteLocked(filename, []byte(toWrite)); err != nil {
		t.Fatalf("Failed to write: %s", err)
	}

	read, err := ReadLocked(filename)
	if err != nil {
		t.Fatalf("Failed to read: %s", err)
	}
	sread := string(read)

	if toWrite != sread {
		t.Errorf("Expected read data to be '%s' but was '%s'", toWrite, sread)
	}
}
