package exporter

import "time"

// ExportSource[T] provides a factory style contract for creating new `T` instances for exporting.
type ExportSource[T any] interface {
	Make(timestamp time.Time) *T

	// Name returns the name of the ExportSource.
	Name() string
}

// ComputeSource[T] provides an interface for a compute data source.
type ComputeSource[T any] interface {
	// CanCompute should return true iff the ComputeSource can effectively act as
	// a source of T data for the given time range. For example, a ComputeSource
	// with two-day coverage cannot fulfill a range from three days ago, and should
	// not be left to return an error in Compute. Instead, it should report that is
	// cannot compute and allow another Source to handle the computation.
	CanCompute(start, end time.Time) bool

	// Compute should compute a single T for the given time range
	Compute(start, end time.Time) (*T, error)

	// Name returns the name of the ComputeSource
	Name() string
}
