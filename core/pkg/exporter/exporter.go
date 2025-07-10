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
type Exporter[TimeUnit any, T any] interface {
	// Export performs the export operation for the provided data.
	Export(time TimeUnit, data *T) error
}

// EventExporter[T] is an alias type of an Exporter[time.Time, T] that writes data that is timestamped.
type EventExporter[T any] Exporter[time.Time, T]

// ComputeExporter[T] is an alias type of an Exporter[opencost.Window, T] that writes data for a specific window.
type ComputeExporter[T any] Exporter[opencost.Window, T]

// EventStorageExporter[T] is an implementation of an Exporter[T] that writes data to a storage backend using
// the `github.com/opencost/opencost/core/pkg/storage` package, a pathing strategy, and an encoder.
type EventStorageExporter[T any] struct {
	paths   pathing.StoragePathFormatter[time.Time]
	encoder Encoder[T]
	storage storage.Storage
}

// NewEventStorageExporter creates a new StorageExporter instance, which is responsible for exporting data to a storage backend.
// It uses a pathing strategy to determine the storage location, an encoder to convert the data to binary format, and
// a storage backend to write the data.
func NewEventStorageExporter[T any](
	paths pathing.StoragePathFormatter[time.Time],
	encoder Encoder[T],
	storage storage.Storage,
) EventExporter[T] {
	return &EventStorageExporter[T]{
		paths:   paths,
		encoder: encoder,
		storage: storage,
	}
}

// Export performs the export operation for the provided data. It encodes the data using the encoder and writes it to
// the storage backend using the pathing strategy.
func (se *EventStorageExporter[T]) Export(t time.Time, data *T) error {
	path := se.paths.ToFullPath("", t, se.encoder.FileExt())

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

// ComputeStorageExporter[T] is an implementation of ComputeExporter[T] that writes data to a storage backend using
// `github.com/opencost/opencost/core/pkg/storage`, a pathing strategy, and an encoder.
type ComputeStorageExporter[T any] struct {
	resolution time.Duration
	paths      pathing.StoragePathFormatter[opencost.Window]
	encoder    Encoder[T]
	storage    storage.Storage
	validator  validator.ExportValidator[T]
}

// NewComputeStorageExporter creates a new ComputeStorageExporter instance, which is responsible for exporting
// data for a specific window to a storage backend. It uses a pathing strategy to determine the storage location,
// an encoder to convert the data to binary format, and a validator to check the data before export. The pipeline
// name and resolution are also provided to help identify the data being exported.
func NewComputeStorageExporter[T any](
	paths pathing.StoragePathFormatter[opencost.Window],
	encoder Encoder[T],
	storage storage.Storage,
	validator validator.ExportValidator[T],
) ComputeExporter[T] {
	return &ComputeStorageExporter[T]{
		paths:     paths,
		encoder:   encoder,
		storage:   storage,
		validator: validator,
	}
}

// Export performs validation on the provided window and data, determines if it should overwrite existing data,
// and stores the data in the location specified by the pathing formatter.
func (se *ComputeStorageExporter[T]) Export(window opencost.Window, data *T) error {
	if se.validator != nil {
		err := se.validator.Validate(window, data)
		if err != nil {
			return fmt.Errorf("failed to validate data: %w", err)
		}
	}

	path := se.paths.ToFullPath("", window, se.encoder.FileExt())

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
