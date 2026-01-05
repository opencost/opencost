package validator

import (
	"errors"
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
)

var (
	// ErrNilSet is used as a validation error when the set passed is nil.
	ErrNilSet error = errors.New("invalid set: nil")

	// ErrNilWindowStart is used as a validation error when the set passed
	// has an open Window Start.
	ErrNilWindowStart error = errors.New("invalid set: nil window.Start")

	// ErrNilWindowEnd is used as a validation error when the set passed
	// has an open Window End.
	ErrNilWindowEnd error = errors.New("invalid set: nil window.End")

	// ErrEmptySet is used as a validation error when the set passed is
	// empty.
	ErrEmptySet error = errors.New("invalid set: empty")
)

// SetConstraint is a helper constraint for an Export[T] implementation
type SetConstraint[T any] interface {
	IsEmpty() bool
	*T
}

// Validator is an implementation of an object capable of validating a T instance prior to
// insertion into a store.
type ExportValidator[T any] interface {
	// Validate determines whether or not the given data can be legally
	// added to the store.
	Validate(window opencost.Window, data *T) error

	// IsOverwrite determines whether or not the provided data can be used
	// to overwrite existing data in the storage.
	IsOverwrite(data *T) bool
}

// validation of a window, which is a common pattern in the validator implementations
func validateWindow(window opencost.Window) (start, end time.Time, err error) {
	s, e := window.Start(), window.End()
	if s == nil {
		err = ErrNilWindowStart
		return
	}
	if e == nil {
		err = ErrNilWindowEnd
		return
	}

	start = *s
	end = *e

	return
}

//--------------------------------------------------------------------------
//  Chain Validator
//--------------------------------------------------------------------------

// chain validator is used to chain multiple validators together.
type chainValidator[T any] struct {
	validators []ExportValidator[T]
}

// NewChainValidator creates a single validator instances which chains together many validators.
func NewChainValidator[T any](validators ...ExportValidator[T]) ExportValidator[T] {
	return &chainValidator[T]{validators: validators}
}

func (cv *chainValidator[T]) Validate(window opencost.Window, data *T) error {
	for _, validator := range cv.validators {
		err := validator.Validate(window, data)
		if err != nil {
			return err
		}
	}
	return nil
}

func (cv *chainValidator[T]) IsOverwrite(data *T) bool {
	for _, validator := range cv.validators {
		if !validator.IsOverwrite(data) {
			return false
		}
	}
	return true
}

//--------------------------------------------------------------------------
//  Set Validator
//--------------------------------------------------------------------------

// setValidator is used for the a potentially "empty" set of data that should avoid
// overwriting existing data in the store, and applies a window and resolution validation.
type setValidator[T any, U SetConstraint[T]] struct {
	resolution time.Duration
}

// NewSetValidator is used for the a potentially "empty" set of data that should avoid
// overwriting existing data in the store, and applies a window and resolution validation.
func NewSetValidator[T any, U SetConstraint[T]](resolution time.Duration) ExportValidator[T] {
	return &setValidator[T, U]{
		resolution: resolution,
	}
}

// IsValid determines whether the provided start and end times are valid for the data provided.
func (sv *setValidator[T, U]) Validate(window opencost.Window, data *T) error {
	if data == nil {
		return ErrNilSet
	}

	start, end, err := validateWindow(window)
	if err != nil {
		return err
	}

	// Check Resolution
	resolution := end.Sub(start)
	if resolution != sv.resolution {
		return fmt.Errorf("invalid set: resolution of %ds != %ds", uint64(resolution.Seconds()), uint64(sv.resolution.Seconds()))
	}

	// Check UTC Multiple
	nearestUTCMultiple := opencost.RoundBack(start.UTC(), sv.resolution)
	if !start.Equal(nearestUTCMultiple) {
		return fmt.Errorf("invalid set: start %s is not a UTC multiple of resolution %ds, the nearest valid start is %s", start.String(), uint64(sv.resolution.Seconds()), nearestUTCMultiple.String())
	}

	return nil
}

// IsOverwrite should return true if the data is not nil and the set is not empty
func (sv *setValidator[T, U]) IsOverwrite(data *T) bool {
	var set U = data

	return set != nil && !set.IsEmpty()
}
