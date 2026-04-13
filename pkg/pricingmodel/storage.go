package pricingmodel

import (
	"fmt"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/exporter"
	"github.com/opencost/opencost/core/pkg/exporter/pathing"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/model/pricingmodel"
	"github.com/opencost/opencost/core/pkg/pipelines"
	"github.com/opencost/opencost/core/pkg/storage"
)

// storageWriter wraps a Storage backend with a StaticFileStoragePathFormatter,
// translating source keys into full storage paths on write.
type storageWriter struct {
	store   storage.Storage
	encoder exporter.Encoder[pricingmodel.PricingModelSet]
	pathing *pathing.StaticFileStoragePathFormatter
}

func newStorageWriter(store storage.Storage, appName string) (*storageWriter, error) {
	p, err := pathing.NewStaticFileStoragePathFormatter(appName, pipelines.PricingModelPipelineName)
	if err != nil {
		return nil, fmt.Errorf("newStorageWriter: failed to create path formatter: %w", err)
	}
	return &storageWriter{
		store:   store,
		encoder: exporter.NewVersionBingenEncoder[pricingmodel.PricingModelSet](pricingmodel.DefaultCodecVersion),
		pathing: p,
	}, nil
}

func (sw *storageWriter) Write(pms *pricingmodel.PricingModelSet) error {
	fullPath := sw.pathing.ToFullPath("", pms.SourceKey, sw.encoder.FileExt())
	data, err := sw.encoder.Encode(pms)
	if err != nil {
		return fmt.Errorf("failed to encode data: %w", err)
	}
	err = sw.store.Write(fullPath, data)
	if err != nil {
		return fmt.Errorf("failed to write to storage: %w", err)
	}
	log.Infof("PricingModel[%s]: exported pricing model set (%d bytes)", pms.SourceKey, len(data))
	return nil
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
		nameParts := strings.Split(f.Name, ".")
		key := nameParts[0]
		if modTime, ok := result[key]; ok && modTime.After(f.ModTime) {
			continue
		}
		result[key] = f.ModTime
	}

	return result, nil
}
