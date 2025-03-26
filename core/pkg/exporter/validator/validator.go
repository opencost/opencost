package validator

import (
	"encoding"
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

// SetConstraint is a helper constraint for StorageStrategy
type SetConstraint[T any] interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler

	Clone() *T
	GetWindow() opencost.Window
	IsEmpty() bool

	*T
}

// Validator is an implementation of an object capable of validating a T instance prior to
// insertion into a store.
type StoreValidator[T any] interface {
	// IsValid determines whether or not the given data can be legally
	// added to the store.
	IsValid(*T) (bool, error)
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
//  Window Validator
//--------------------------------------------------------------------------

// windowValidator is a StoreValidator implementation which ensures that all
// set windows are closed.
type windowValidator[T any, U SetConstraint[T]] struct{}

// NewWindowValidator creates a new window validator that ensures all
// set windows are closed.
func NewWindowValidator[T any, U SetConstraint[T]]() StoreValidator[T] {
	return &windowValidator[T, U]{}
}

// IsValid determines whether or not the given data can be legally
// added to the store.
func (wv *windowValidator[T, U]) IsValid(t *T) (bool, error) {
	if t == nil {
		return false, ErrNilSet
	}

	var set U = t
	_, _, err := validateWindow(set.GetWindow())
	if err != nil {
		return false, err
	}

	return true, nil
}

//--------------------------------------------------------------------------
//  Resolution Validator
//--------------------------------------------------------------------------

// resolution validator is used to validate against window and the window resolution
type resolutionValidator[T any, U SetConstraint[T]] struct {
	resolution time.Duration
}

// NewResolutionValidator creates a new validator for storage sets that validate both the window
// and whether the resolution matches the window.
func NewResolutionValidator[T any, U SetConstraint[T]](resolution time.Duration) StoreValidator[T] {
	return &resolutionValidator[T, U]{
		resolution: resolution,
	}
}

// IsValid determines whether or not the given data can be legally
// added to the store.
func (rv *resolutionValidator[T, U]) IsValid(t *T) (bool, error) {
	if t == nil {
		return false, ErrNilSet
	}

	var set U = t
	start, end, err := validateWindow(set.GetWindow())
	if err != nil {
		return false, err
	}

	resolution := end.Sub(start)
	if resolution != rv.resolution {
		return false, fmt.Errorf("invalid set: resolution of %ds != %ds", uint64(resolution.Seconds()), uint64(rv.resolution.Seconds()))
	}

	return true, nil
}

//--------------------------------------------------------------------------
//  UTC Resolution Validator
//--------------------------------------------------------------------------

// utc resolution validator is used to validate against window and the window resolution, and checks that the window
// start and end are on the UTC multiple for that resolution
type utcResolutionValidator[T any, U SetConstraint[T]] struct {
	resolution time.Duration
}

// NewUTCResolutionValidator creates a new validator for storage sets that validate both the window,
// whether the resolution matches the window and that the window is a UTC multiple of the resolution.
func NewUTCResolutionValidator[T any, U SetConstraint[T]](resolution time.Duration) StoreValidator[T] {
	return &utcResolutionValidator[T, U]{
		resolution: resolution,
	}
}

// IsValid determines whether or not the given data can be legally
// added to the store.
func (urv *utcResolutionValidator[T, U]) IsValid(t *T) (bool, error) {
	if t == nil {
		return false, ErrNilSet
	}

	// Check Valid Window
	var set U = t
	start, end, err := validateWindow(set.GetWindow())
	if err != nil {
		return false, err
	}

	// Check Resolution
	resolution := end.Sub(start)
	if resolution != urv.resolution {
		return false, fmt.Errorf("invalid set: resolution of %ds != %ds", uint64(resolution.Seconds()), uint64(urv.resolution.Seconds()))
	}

	// Check UTC Multiple
	nearestUTCMultiple := opencost.RoundBack(start.UTC(), urv.resolution)
	if !start.Equal(nearestUTCMultiple) {
		return false, fmt.Errorf("invalid set: start %s is not a UTC multiple of resolution %ds, the nearest valid start is %s", start.String(), uint64(urv.resolution.Seconds()), nearestUTCMultiple.String())
	}

	return true, nil
}

//--------------------------------------------------------------------------
//  Empty Set Validator
//--------------------------------------------------------------------------

// emptySetValidator validates that a set is non empty, has a valid window,
// and
type emptySetValidator[T any, U SetConstraint[T]] struct {
	resolution time.Duration
}

// NewEmptySetValidator creates a validator that checks for non-empty sets,
// a valid window, and a valid resolution
func NewEmptySetValidator[T any, U SetConstraint[T]](resolution time.Duration) StoreValidator[T] {
	return &emptySetValidator[T, U]{
		resolution: resolution,
	}
}

// IsValid determines whether or not the given data can be legally
// added to the store.
func (esv *emptySetValidator[T, U]) IsValid(t *T) (bool, error) {
	// non-nil validation
	if t == nil {
		return false, ErrNilSet
	}

	var set U = t
	// non-empty validation
	if set.IsEmpty() {
		return false, ErrEmptySet
	}

	// window validation
	start, end, err := validateWindow(set.GetWindow())
	if err != nil {
		return false, err
	}

	// resolution validation
	resolution := end.Sub(start)
	if resolution != esv.resolution {
		return false, fmt.Errorf("invalid set: resolution of %ds != %ds", uint64(resolution.Seconds()), uint64(esv.resolution.Seconds()))
	}

	return true, nil
}
