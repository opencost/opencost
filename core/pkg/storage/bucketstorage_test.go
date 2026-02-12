package storage

import (
	"fmt"
	"os"
	"testing"
)

// This suite of integration tests is meant to validate if an implementation of Storage that relies on a could
// bucket service properly implements the interface. To run these tests the env variable "TEST_BUCKET_CONFIG"
// must be set with the path to a valid bucket config as defined in the NewBucketStorage() function.
func createStorage(configPath string) (Storage, error) {

	bytes, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}
	store, err := NewBucketStorage(bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize storage: %w", err)
	}

	return store, nil
}

func TestBucketStorage_List(t *testing.T) {
	configPath := os.Getenv("TEST_BUCKET_CONFIG")
	if configPath == "" {
		t.Skip("skipping integration test, set environment variable TEST_BUCKET_CONFIG")
	}
	store, err := createStorage(configPath)
	if err != nil {
		t.Errorf("failed to create storage: %s", err.Error())
		return
	}

	TestStorageList(t, store)
}

func TestBucketStorage_ListDirectories(t *testing.T) {
	configPath := os.Getenv("TEST_BUCKET_CONFIG")
	if configPath == "" {
		t.Skip("skipping integration test, set environment variable TEST_BUCKET_CONFIG")
	}
	store, err := createStorage(configPath)
	if err != nil {
		t.Errorf("failed to create storage: %s", err.Error())
		return
	}

	TestStorageListDirectories(t, store)
}

func TestBucketStorage_Exists(t *testing.T) {
	configPath := os.Getenv("TEST_BUCKET_CONFIG")
	if configPath == "" {
		t.Skip("skipping integration test, set environment variable TEST_BUCKET_CONFIG")
	}
	store, err := createStorage(configPath)
	if err != nil {
		t.Errorf("failed to create storage: %s", err.Error())
		return
	}

	TestStorageExists(t, store)
}

func TestBucketStorage_Read(t *testing.T) {
	configPath := os.Getenv("TEST_BUCKET_CONFIG")
	if configPath == "" {
		t.Skip("skipping integration test, set environment variable TEST_BUCKET_CONFIG")
	}
	store, err := createStorage(configPath)
	if err != nil {
		t.Errorf("failed to create storage: %s", err.Error())
		return
	}

	TestStorageRead(t, store)
}

func TestBucketStorage_Stat(t *testing.T) {
	configPath := os.Getenv("TEST_BUCKET_CONFIG")
	if configPath == "" {
		t.Skip("skipping integration test, set environment variable TEST_BUCKET_CONFIG")
	}
	store, err := createStorage(configPath)
	if err != nil {
		t.Errorf("failed to create storage: %s", err.Error())
		return
	}

	TestStorageStat(t, store)
}

// We should be able to call validate function with and without write and delete check without any errors
func TestBucketStorage_Validate(t *testing.T) {
	configPath := os.Getenv("TEST_BUCKET_CONFIG")
	if configPath == "" {
		t.Skip("skipping integration test, set environment variable TEST_BUCKET_CONFIG")
	}
	store, err := createStorage(configPath)
	if err != nil {
		t.Errorf("failed to create storage: %s", err.Error())
		return
	}

	// Validate BucketStorage with write and delete check
	err = Validate(store, true)
	if err != nil {
		t.Errorf("failed to validate storage: %s", err.Error())
	}

	// Validate BucketStorage without write and delete check (should not fail)
	err = Validate(store, false)
	if err != nil {
		t.Errorf("failed to validate storage: %s", err.Error())
	}
}
