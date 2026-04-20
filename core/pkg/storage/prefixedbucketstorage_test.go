package storage

import "testing"

func TestPrefixedBucketStorage_ReadToLocalFile(t *testing.T) {
	base := NewMemoryStorage()
	store, err := NewPrefixedBucketStorage(base, "myprefix")
	if err != nil {
		t.Fatalf("failed to create prefixed storage: %s", err)
	}

	TestStorageReadToLocalFile(t, store)
}

func TestPrefixedBucketStorage_ReadStream(t *testing.T) {
	base := NewMemoryStorage()
	store, err := NewPrefixedBucketStorage(base, "myprefix")
	if err != nil {
		t.Fatalf("failed to create prefixed storage: %s", err)
	}

	TestStorageReadStream(t, store)
}
