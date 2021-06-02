package util

import (
	"fmt"
	"strconv"
	"sync"
	"time"
)

const (
	// SecsPerMin expresses the amount of seconds in a minute
	SecsPerMin = 60.0

	// SecsPerHour expresses the amount of seconds in a minute
	SecsPerHour = 3600.0

	// SecsPerDay expressed the amount of seconds in a day
	SecsPerDay = 86400.0

	// MinsPerHour expresses the amount of minutes in an hour
	MinsPerHour = 60.0

	// MinsPerDay expresses the amount of minutes in a day
	MinsPerDay = 1440.0

	// HoursPerDay expresses the amount of hours in a day
	HoursPerDay = 24.0

	// HoursPerMonth expresses the amount of hours in a month
	HoursPerMonth = 730.0

	// DaysPerMonth expresses the amount of days in a month
	DaysPerMonth = 30.42
)

// DurationString converts a duration to a Prometheus-compatible string in
// terms of days, hours, minutes, or seconds.
func DurationString(duration time.Duration) string {
	durSecs := int64(duration.Seconds())

	durStr := ""
	if durSecs > 0 {
		if durSecs%SecsPerDay == 0 {
			// convert to days
			durStr = fmt.Sprintf("%dd", durSecs/SecsPerDay)
		} else if durSecs%SecsPerHour == 0 {
			// convert to hours
			durStr = fmt.Sprintf("%dh", durSecs/SecsPerHour)
		} else if durSecs%SecsPerMin == 0 {
			// convert to mins
			durStr = fmt.Sprintf("%dm", durSecs/SecsPerMin)
		} else if durSecs > 0 {
			// default to mins, as long as duration is positive
			durStr = fmt.Sprintf("%ds", durSecs)
		}
	}

	return durStr
}

// DurationOffsetStrings converts a (duration, offset) pair to Prometheus-
// compatible strings in terms of days, hours, minutes, or seconds.
func DurationOffsetStrings(duration, offset time.Duration) (string, string) {
	return DurationString(duration), DurationString(offset)
}

// ParseDuration converts a Prometheus-style duration string into a Duration
func ParseDuration(duration string) (*time.Duration, error) {
	unitStr := duration[len(duration)-1:]
	var unit time.Duration
	switch unitStr {
	case "s":
		unit = time.Second
	case "m":
		unit = time.Minute
	case "h":
		unit = time.Hour
	case "d":
		unit = 24.0 * time.Hour
	default:
		return nil, fmt.Errorf("error parsing duration: %s did not match expected format [0-9+](s|m|d|h)", duration)
	}

	amountStr := duration[:len(duration)-1]
	amount, err := strconv.ParseInt(amountStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("error parsing duration: %s did not match expected format [0-9+](s|m|d|h)", duration)
	}

	dur := time.Duration(amount) * unit
	return &dur, nil
}

// ParseTimeRange returns a start and end time, respectively, which are converted from
// a duration and offset, defined as strings with Prometheus-style syntax.
func ParseTimeRange(duration, offset string) (*time.Time, *time.Time, error) {
	// endTime defaults to the current time, unless an offset is explicity declared,
	// in which case it shifts endTime back by given duration
	endTime := time.Now()
	if offset != "" {
		o, err := ParseDuration(offset)
		if err != nil {
			return nil, nil, fmt.Errorf("error parsing offset (%s): %s", offset, err)
		}
		endTime = endTime.Add(-1 * *o)
	}

	// if duration is defined in terms of days, convert to hours
	// e.g. convert "2d" to "48h"
	durationNorm, err := normalizeTimeParam(duration)
	if err != nil {
		return nil, nil, fmt.Errorf("error parsing duration (%s): %s", duration, err)
	}

	// convert time duration into start and end times, formatted
	// as ISO datetime strings
	dur, err := time.ParseDuration(durationNorm)
	if err != nil {
		return nil, nil, fmt.Errorf("errorf parsing duration (%s): %s", durationNorm, err)
	}
	startTime := endTime.Add(-1 * dur)

	return &startTime, &endTime, nil
}

func normalizeTimeParam(param string) (string, error) {
	// convert days to hours
	if param[len(param)-1:] == "d" {
		count := param[:len(param)-1]
		val, err := strconv.ParseInt(count, 10, 64)
		if err != nil {
			return "", err
		}
		val = val * 24
		param = fmt.Sprintf("%dh", val)
	}

	return param, nil
}

// FormatStoreResolution provides a clean notation for ETL store resolutions.
// e.g. daily => 1d; hourly => 1h
func FormatStoreResolution(dur time.Duration) string {
	if dur >= 24*time.Hour {
		return fmt.Sprintf("%dd", int(dur.Hours()/24.0))
	} else if dur >= time.Hour {
		return fmt.Sprintf("%dh", int(dur.Hours()))
	}
	return fmt.Sprint(dur)
}

// JobTicker is a ticker used to synchronize the next run of a repeating
// process. The designated use-case is for infinitely-looping selects,
// where a timeout or an exit channel might cancel the process, but otherwise
// the intent is to wait at the select for some amount of time until the
// next run. This differs from a standard ticker, which ticks without
// waiting and drops any missed ticks; rather, this ticker must be kicked
// off manually for each tick, so that after the current run of the job
// completes, the timer starts again.
type JobTicker struct {
	Ch     <-chan time.Time
	ch     chan time.Time
	closed bool
	mx     sync.Mutex
}

// NewJobTicker instantiates a new JobTicker.
func NewJobTicker() *JobTicker {
	c := make(chan time.Time)

	return &JobTicker{
		Ch:     c,
		ch:     c,
		closed: false,
	}
}

// Close closes the JobTicker channels
func (jt *JobTicker) Close() {
	jt.mx.Lock()
	defer jt.mx.Unlock()

	if jt.closed {
		return
	}

	jt.closed = true
	close(jt.ch)
}

// TickAt schedules the next tick of the ticker for the given time in the
// future. If the time is not in the future, the ticker will tick immediately.
func (jt *JobTicker) TickAt(t time.Time) {
	go func(t time.Time) {
		n := time.Now()
		if t.After(n) {
			time.Sleep(t.Sub(n))
		}

		jt.mx.Lock()
		defer jt.mx.Unlock()

		if !jt.closed {
			jt.ch <- time.Now()
		}
	}(t)
}

// TickIn schedules the next tick of the ticker for the given duration into
// the future. If the duration is less than or equal to zero, the ticker will
// tick immediately.
func (jt *JobTicker) TickIn(d time.Duration) {
	go func(d time.Duration) {
		if d > 0 {
			time.Sleep(d)
		}

		jt.mx.Lock()
		defer jt.mx.Unlock()

		if !jt.closed {
			jt.ch <- time.Now()
		}
	}(d)
}
