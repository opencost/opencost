package storage

import (
	"fmt"
	"os"

	"github.com/opencost/opencost/core/pkg/env"
)

// GetDefaultStorage initializes the default shared storage which is required for kubecost
func GetDefaultStorage() Storage {
	store, err := InitializeStorage(env.GetDefaultStorageConfigFilePath())
	if err != nil {
		panic(fmt.Sprintf("failed to initialize default storage: %s", err.Error()))
	}
	return store
}

// InitializeStorage creates a storage from the config file at the given path
func InitializeStorage(configPath string) (Storage, error) {
	storageConfig, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file '%s': %w", configPath, err)
	}
	store, err := NewBucketStorage(storageConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage from config '%s': %w", configPath, err)
	}

	return store, nil
}
