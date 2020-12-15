package util

import (
	"fmt"
	"strconv"
	"time"
)

const (
	// MinsPerHour expresses the amount of minutes in an hour
	MinsPerHour = 60.0

	// HoursPerDay expresses the amount of hours in a day
	HoursPerDay = 24.0

	// HoursPerMonth expresses the amount of hours in a month
	HoursPerMonth = 730.0

	// DaysPerMonth expresses the amount of days in a month
	DaysPerMonth = 30.42
)

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
