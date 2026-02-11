package kubemodel

import (
	"errors"
	"fmt"
	"math"
)

// @bingen:generate:StatType
type StatType string

const (
	StatAvg StatType = "avg"
	StatMax StatType = "max"
	StatMin StatType = "min"
	StatP95 StatType = "p95"
	StatP85 StatType = "p85"
)

// @bingen:generate:Stats
type Stats map[StatType]float64

func NewStats(capacity ...int) Stats {
	if len(capacity) == 1 {
		s := make(map[StatType]float64, capacity[0])
		return s
	}

	return map[StatType]float64{}
}

func (s Stats) Avg() (float64, bool) {
	if s == nil {
		return 0.0, false
	}

	val, ok := s[StatAvg]

	return val, ok
}

func (s Stats) Max() (float64, bool) {
	if s == nil {
		return 0.0, false
	}

	val, ok := s[StatMax]

	return val, ok
}

func (s Stats) Min() (float64, bool) {
	if s == nil {
		return 0.0, false
	}

	val, ok := s[StatMin]

	return val, ok
}

func (s Stats) P95() (float64, bool) {
	if s == nil {
		return 0.0, false
	}

	val, ok := s[StatP95]

	return val, ok
}

func (s Stats) P85() (float64, bool) {
	if s == nil {
		return 0.0, false
	}

	val, ok := s[StatP85]

	return val, ok
}

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
