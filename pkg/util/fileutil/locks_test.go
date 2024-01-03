package fileutil

import (
	"os"
	"path/filepath"
	"testing"
)

// Make sure read works on file created without locking logic
func TestReadLocked(t *testing.T) {
	toWrite := "hello world"
	dir := t.TempDir()
	filename := filepath.Join(dir, "test.txt")
	if err := os.WriteFile(filename, []byte(toWrite), 0600); err != nil {
		t.Fatalf("failed to write test data: %s", err)
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
