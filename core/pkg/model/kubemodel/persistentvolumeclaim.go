package kubemodel

import "time"

// TODO complete
type PersistentVolumeClaim struct {
	UID                        string
	PersistentVolumeUID        string
	Name                       string
	Start                      time.Time
	End                        time.Time
	StorageCapacityByteSeconds uint64
}
