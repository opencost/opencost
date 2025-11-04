package kubemodel

import "time"

// TODO complete
// TODO uint64 or float64 for numbers?
type PersistentVolume struct {
	UID                        string
	ClusterUID                 string
	Name                       string
	Start                      time.Time
	End                        time.Time
	StorageCapacityByteSeconds uint64
}
