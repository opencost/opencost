package util

import (
	"fmt"
	"time"
)

type ResolutionConfiguration struct {
	Interval  string
	Retention int
}

// Resolution is a utility for maintaining a set of windows that span the time period defined by the interval and
// with a count of retention.
type Resolution struct {
	interval    Interval
	intervalDef string
	retention   int
}

func NewResolution(configuration ResolutionConfiguration) (*Resolution, error) {
	interval, err := NewInterval(configuration.Interval)
	if err != nil {
		return nil, fmt.Errorf("failed to create resolution: %w", err)
	}
	return &Resolution{
		interval:    interval,
		intervalDef: configuration.Interval,
		retention:   configuration.Retention,
	}, nil
}

// Retention is a getter which returns the retention of the Resolution
func (r *Resolution) Retention() int {
	return r.retention
}

// Interval is a getter which returns the interval definition string of the Resolution
func (r *Resolution) Interval() string {
	return r.intervalDef
}

// Current returns the time that the current interval began
func (r *Resolution) Current() time.Time {
	return r.interval.Truncate(time.Now())
}

// Next returns the time that the next interval will start at
func (r *Resolution) Next() time.Time {
	return r.interval.Add(r.interval.Truncate(time.Now()), 1)
}

// Limit returns the time that oldest interval in retention began
func (r *Resolution) Limit() time.Time {
	return r.interval.Add(r.interval.Truncate(time.Now()), -(r.retention - 1))
}

// Get returns the interval start time for the given time
func (r *Resolution) Get(t time.Time) time.Time {
	return r.interval.Truncate(t)
}
