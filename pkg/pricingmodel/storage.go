package pricingmodel

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/exporter/pathing"
	"github.com/opencost/opencost/core/pkg/pipelines"
	"github.com/opencost/opencost/core/pkg/storage"
)

// storageWriter wraps a Storage backend with a StaticFileStoragePathFormatter,
// translating source keys into full storage paths on write.
type storageWriter struct {
	store   storage.Storage
	pathing *pathing.StaticFileStoragePathFormatter
}

func newStorageWriter(store storage.Storage, appName string) (*storageWriter, error) {
	p, err := pathing.NewStaticFileStoragePathFormatter(appName, pipelines.PricingModelPipelineName)
	if err != nil {
		return nil, fmt.Errorf("newStorageWriter: failed to create path formatter: %w", err)
	}
	return &storageWriter{
		store:   store,
		pathing: p,
	}, nil
}

func (sw *storageWriter) Write(sourceKey string, data []byte) error {
	fullPath := sw.pathing.ToFullPath("", sourceKey, "")
	return sw.store.Write(fullPath, data)
}

// LastUpdates returns a map of source key to last modified time for each file
// found under the formatter's directory. Source keys are reconstructed as the
// file path relative to Dir().
func (sw *storageWriter) LastUpdates() (map[string]time.Time, error) {
	result := make(map[string]time.Time)
	dir := sw.pathing.Dir()

	files, err := sw.store.List(dir)
	if err != nil && !storage.IsNotExist(err) {
		return nil, fmt.Errorf("collectModTimes: listing %s: %w", dir, err)
	}
	for _, f := range files {
		key := f.Name
		result[key] = f.ModTime
	}

	return result, nil
}
