package kubemodel

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/exporter"
	"github.com/opencost/opencost/core/pkg/exporter/pathing"
	coremodel "github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/storage"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

// supportedResolutions lists the resolutions written by the pipeline, in ascending order.
var supportedResolutions = []time.Duration{time.Hour, timeutil.Day}

// Querier reads KubeModelSets written by the pipeline from storage.
type Querier struct {
	appName   string
	clusterId string
	store     storage.Storage
}

// NewQuerier creates a Querier backed by the given storage and cluster.
func NewQuerier(appName, clusterId string, store storage.Storage) *Querier {
	return &Querier{store: store, appName: appName, clusterId: clusterId}
}

// Query returns KubeModelSets covering the given window split into resolution-sized sub-windows.
// resolution is snapped to the nearest supported pipeline resolution (1h or 1d).
// Sub-windows with no file in storage are silently skipped.
func (q *Querier) Query(window opencost.Window) ([]*coremodel.KubeModelSet, error) {
	if window.IsOpen() {
		return nil, fmt.Errorf("kubemodel querier: window must be closed")
	}

	res := snapResolution(window)
	resStr := timeutil.FormatStoreResolution(res)
	formatter, err := pathing.NewKubeModelStoragePathFormatter(q.appName, q.clusterId, resStr)
	if err != nil {
		return nil, fmt.Errorf("kubemodel querier: %w", err)
	}

	start := window.Start().Truncate(res)
	end := window.End().Truncate(res)
	subWindows, err := opencost.GetWindowsForQueryWindow(start, end, res)
	if err != nil {
		return nil, fmt.Errorf("kubemodel querier: splitting window: %w", err)
	}

	results := make([]*coremodel.KubeModelSet, 0, len(subWindows))
	for _, w := range subWindows {
		kms, err := q.readWindow(formatter, w)
		if err != nil {
			if storage.IsNotExist(err) {
				continue
			}
			return nil, fmt.Errorf("kubemodel querier: reading window %s: %w", w, err)
		}
		results = append(results, kms)
	}

	return results, nil
}

func (q *Querier) readWindow(formatter pathing.StoragePathFormatter[opencost.Window], window opencost.Window) (*coremodel.KubeModelSet, error) {
	path := formatter.ToFullPath("", window, exporter.BingenExt)

	data, err := q.store.Read(path)
	if err != nil {
		return nil, err
	}

	kms := new(coremodel.KubeModelSet)
	if err := kms.UnmarshalBinary(data); err != nil {
		return nil, fmt.Errorf("decoding KubeModelSet: %w", err)
	}

	return kms, nil
}

// snapResolution returns the largest supported resolution that evenly divides
// the window duration. Falls back to the smallest supported resolution if none
// divides evenly.
func snapResolution(window opencost.Window) time.Duration {
	dur := window.Duration()
	for i := len(supportedResolutions) - 1; i >= 0; i-- {
		if dur%supportedResolutions[i] == 0 {
			return supportedResolutions[i]
		}
	}
	return supportedResolutions[0]
}
