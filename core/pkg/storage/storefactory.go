package storage

import (
	"fmt"
	"os"

	"github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/core/pkg/log"
)

// GetDefaultStorage initializes the default shared storage which is required for kubecost. Panics
// if the storage cannot be initialized.
func GetDefaultStorage() Storage {
	store, err := InitializeStorage(env.GetDefaultStorageConfigFilePath())
	if err != nil {
		panic(fmt.Sprintf("failed to initialize default storage: %s", err.Error()))
	}
	return store
}

// GetConfiguredStorage retrieves the default shared storage which is required for running an opencost.
func GetConfiguredStorage() Storage {
	const warningMessage = `Failed to create local directory '%s' - %s.
		Did you mean to enable the collector? For persistent storage, it's recommended to use Prometheus, 
		or set a storage bucket configuration at %s. 

		%s`

	// Try bucket storage if it exists
	store, err := TryGetDefaultStorage()
	if err == nil {
		return store
	}

	// Fallback to a local storage bucket
	dir := env.GetConfigPath()
	err = os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		log.Warnf(
			warningMessage,
			dir,
			err.Error(),
			env.GetDefaultStorageConfigFilePath(),
			"Falling back to an in-memory file system for collector, which will lose any persistent storage upon restart.",
		)

		return NewMemoryStorage()
	}

	return NewFileStorage(dir)
}

// TryGetDefaultStorage will attempt to load the default bucket configuration, but will not panic
// if the config file does not exist.
func TryGetDefaultStorage() (Storage, error) {
	store, err := InitializeStorage(env.GetDefaultStorageConfigFilePath())
	if err != nil {
		return nil, fmt.Errorf("failed to initialize default storage: %w", err)
	}
	return store, nil
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
