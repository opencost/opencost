package kubemodel

import (
	"errors"
	"fmt"
	"math"
)

// StatType represents the type of statistical measurement
type StatType string // @bingen:generate[type=enum]

const (
	Avg StatType = "avg" // @bingen:field[version=1]
	Max StatType = "max" // @bingen:field[version=1]
	Min StatType = "min" // @bingen:field[version=1]
	P95 StatType = "p95" // @bingen:field[version=1]
	P85 StatType = "p85" // @bingen:field[version=1]
)

// Stats is a map of statistical measurements
type Stats map[StatType]float64 // @bingen:generate[type=map]

// NewStats creates a new Stats instance with optional pre-allocated capacity
func NewStats(capacity ...int) Stats {
	if len(capacity) == 1 {
		s := make(map[StatType]float64, capacity[0])
		return s
	}

	return map[StatType]float64{}
}

// Avg returns the average statistic value and whether it exists
func (s Stats) Avg() (float64, bool) {
	if s == nil {
		return 0.0, false
	}

	val, ok := s[Avg]

	return val, ok
}

// Max returns the maximum statistic value and whether it exists
func (s Stats) Max() (float64, bool) {
	if s == nil {
		return 0.0, false
	}

	val, ok := s[Max]

	return val, ok
}

// Min returns the minimum statistic value and whether it exists
func (s Stats) Min() (float64, bool) {
	if s == nil {
		return 0.0, false
	}

	val, ok := s[Min]

	return val, ok
}

// P95 returns the 95th percentile statistic value and whether it exists
func (s Stats) P95() (float64, bool) {
	if s == nil {
		return 0.0, false
	}

	val, ok := s[P95]

	return val, ok
}

// P85 returns the 85th percentile statistic value and whether it exists
func (s Stats) P85() (float64, bool) {
	if s == nil {
		return 0.0, false
	}

	val, ok := s[P85]

	return val, ok
}

// Sanitize removes invalid floating-point values (NaN, Infinity) and returns an error if any were found
func (s Stats) Sanitize() error {
	if s == nil {
		return nil
	}

	var errs []error

	for t := range s {
		if math.IsNaN(s[t]) {
			delete(s, t)
			errs = append(errs, fmt.Errorf("%v is NaN", t))
		}
		if math.IsInf(s[t], 0) {
			delete(s, t)
			errs = append(errs, fmt.Errorf("%v is Inf", t))
		}
	}

	if len(errs) > 0 {
		errStr := fmt.Sprintf("%d errors:", len(errs))
		for _, e := range errs {
			errStr += fmt.Sprintf(" [%s]", e)
		}
		return errors.New(errStr)
	}

	return nil
}