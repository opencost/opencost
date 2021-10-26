package config

import "github.com/kubecost/cost-model/pkg/storage"

// Configuration
type Configuration interface {
	Get(result interface{}) error
	Set(new interface{}) error
}

type BinaryConfigStore interface {
	Read() ([]byte, error)
	Write([]byte) error
	Stat() (*storage.StorageInfo, error)
}
