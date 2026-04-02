package pricingmodel

import (
	"fmt"

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
