package exporter

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/exporter/pathing"
	"github.com/opencost/opencost/core/pkg/exporter/validator"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/storage"
)

// Exporter[T] is a generic interface for exporting T instances to a specific storage destination.
type Exporter[T any] interface {
	Export(window opencost.Window, data *T) error
}

// StorageExporter[T] is an implementation of Exporter[T] that writes data to a storage backend using
// `github.com/opencost/opencost/core/pkg/storage`, a pathing strategy, and an encoder.
type StorageExporter[T any] struct {
	pipeline   string
	resolution time.Duration
	paths      pathing.StoragePathFormatter
	encoder    Encoder[T]
	storage    storage.Storage
	validator  validator.ExportValidator[T]
}

func NewStorageExporter[T any](
	pipeline string,
	resolution time.Duration,
	paths pathing.StoragePathFormatter,
	encoder Encoder[T],
	storage storage.Storage,
	validator validator.ExportValidator[T],
) *StorageExporter[T] {
	return &StorageExporter[T]{
		pipeline:   pipeline,
		resolution: resolution,
		paths:      paths,
		encoder:    encoder,
		storage:    storage,
		validator:  validator,
	}
}

func (se *StorageExporter[T]) Export(window opencost.Window, data *T) error {
	if se.validator != nil {
		err := se.validator.Validate(window, data)
		if err != nil {
			return fmt.Errorf("failed to validate data: %w", err)
		}
	}

	s, e := *window.Start(), *window.End()
	path := se.paths.ToFullPath("", s, e)

	currentExists, err := se.storage.Exists(path)
	if err != nil {
		return fmt.Errorf("unable to check for existing data from storage path: %w", err)
	}

	if currentExists && se.validator != nil && !se.validator.IsOverwrite(data) {
		log.Debugf("retaining existing data in storage at path: %s", path)
		return nil
	}

	bin, err := se.encoder.Encode(data)
	if err != nil {
		return fmt.Errorf("failed to encode data: %w", err)
	}

	log.Debugf("writing new binary data to storage %s", path)
	err = se.storage.Write(path, bin)
	if err != nil {
		return fmt.Errorf("failed to write binary data to file '%s': %w", path, err)
	}

	return nil
}
