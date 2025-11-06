package kubemodel

import (
	"fmt"
	"math"
)

// StatType represents the type of statistical measurement
type StatType string // @bingen:generate[type=enum]

const (
	StatTypeAvg StatType = "avg" // @bingen:field[version=1]
	StatTypeMax StatType = "max" // @bingen:field[version=1]
	StatTypeMin StatType = "min" // @bingen:field[version=1]
	StatTypeP95 StatType = "p95" // @bingen:field[version=1]
	StatTypeP85 StatType = "p85" // @bingen:field[version=1]
)

// Stats is a map of statistical measurements
type Stats map[StatType]float64 // @bingen:generate[type=map]

// NewStats creates a new Stats instance with optional pre-allocated capacity
func NewStats(capacity ...int) Stats {
	if len(capacity) > 0 {
		return make(Stats, capacity[0])
	}
	return make(Stats)
}

// Avg returns the average statistic value and whether it exists
func (s Stats) Avg() (float64, bool) {
	if s == nil {
		return 0, false
	}
	val, ok := s[StatTypeAvg]
	return val, ok
}

// Max returns the maximum statistic value and whether it exists
func (s Stats) Max() (float64, bool) {
	if s == nil {
		return 0, false
	}
	val, ok := s[StatTypeMax]
	return val, ok
}

// Min returns the minimum statistic value and whether it exists
func (s Stats) Min() (float64, bool) {
	if s == nil {
		return 0, false
	}
	val, ok := s[StatTypeMin]
	return val, ok
}

// P95 returns the 95th percentile statistic value and whether it exists
func (s Stats) P95() (float64, bool) {
	if s == nil {
		return 0, false
	}
	val, ok := s[StatTypeP95]
	return val, ok
}

// P85 returns the 85th percentile statistic value and whether it exists
func (s Stats) P85() (float64, bool) {
	if s == nil {
		return 0, false
	}
	val, ok := s[StatTypeP85]
	return val, ok
}

// Sanitize removes invalid floating-point values (NaN, Infinity) and returns an error if any were found
func (s Stats) Sanitize() error {
	if s == nil {
		return nil
	}

	var invalidStats []string
	for statType, value := range s {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			delete(s, statType)
			invalidStats = append(invalidStats, fmt.Sprintf("%s=%v", statType, value))
		}
	}

	if len(invalidStats) > 0 {
		return fmt.Errorf("removed invalid stat values: %v", invalidStats)
	}

	return nil
}