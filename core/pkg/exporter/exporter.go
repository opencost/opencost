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

type StorageExporter[T any] struct {
	pipeline   string
	resolution time.Duration
	paths      pathing.StoragePathFormatter
	encoder    Encoder[T]
	storage    storage.Storage
	validator  validator.StoreValidator[T]
}

func NewStorageExporter[T any](
	pipeline string,
	resolution time.Duration,
	paths pathing.StoragePathFormatter,
	encoder Encoder[T],
	storage storage.Storage,
	validator validator.StoreValidator[T],
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
	// TODO: Move basic data validation and window validation to StoreValidator[T]
	if data == nil {
		return fmt.Errorf("invalid data: nil")
	}

	if window.IsOpen() {
		return fmt.Errorf("invalid window: open")
	}

	s, e := *window.Start(), *window.End()
	path := se.paths.ToFullPath("", s, e)

	// FIXME: Validator should handle general logic for what to do provided specific circumstances
	// FIXME: like an empty set AND a file already exists. Having a solid set of defaults here with
	// FIXME: a customizable abstraction seems reasonable to start.
	/*
		currentExists, err := se.storage.Exists(path)
		if err != nil {
			return fmt.Errorf("unable to check for existing data from storage path: %w", err)
		}

		if isSetEmpty && currentSetExists {
			log.Debugf("retaining existing data in storage at path: %s", path)
			return nil
		}
	*/

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
