package collector

import (
	"time"

	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
)

// StoreProvider returns an appropriate collector for the given window
type StoreProvider interface {
	GetStore(start, end time.Time) metric.MetricStore
}

// RepoStoreProvider is a StoreProvider implementation which uses a Repository and the Resolutions that it is
// configured with to return the
type RepoStoreProvider struct {
}
