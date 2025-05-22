package util

import (
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

var intervalRegex = regexp.MustCompile(`^(\d+)(s|m|h|d|w)$`)

// Interval is a time period defined by a string with a integer followed by a letter (ex: 5d = 5 days)
type Interval interface {
	// Add adds the interval multiplied by the given int to the given time. (A 10m interval called with 3 would add 30
	// minutes to the given time)
	Add(time.Time, int) time.Time

	// Truncate returns the start of the interval that the given time is a part off
	Truncate(time time.Time) time.Time
}

func NewInterval(def string) (Interval, error) {
	match := intervalRegex.FindStringSubmatch(def)
	if match == nil {
		return nil, fmt.Errorf("failed to parse interval '%s'", def)
	}

	num, err := strconv.ParseInt(match[1], 10, 64)
	// This should not happen
	if err != nil {
		panic(fmt.Sprintf("NewInterval: regex failure on int '%s'", def))
	}

	switch match[2] {
	case "s":
		return &durationInterval{time.Duration(num) * time.Second}, nil
	case "m":
		return &durationInterval{time.Duration(num) * time.Minute}, nil
	case "h":
		return &durationInterval{time.Duration(num) * time.Hour}, nil
	case "d":
		return &durationInterval{time.Duration(num) * timeutil.Day}, nil
	case "w":
		return &weekInterval{int(num)}, nil
	default:
		panic(fmt.Sprintf("NewInterval: regex failure on unit '%s'", def))
	}
}

type durationInterval struct {
	duration time.Duration
}

func (d *durationInterval) Add(t time.Time, i int) time.Time {
	return t.Add(d.duration * time.Duration(i))
}

func (d *durationInterval) Truncate(time time.Time) time.Time {
	return time.UTC().Truncate(d.duration)
}

// weekInterval is an interval that tracks multiples of weeks with the week starting on Sunday
type weekInterval struct {
	count int
}

func (w *weekInterval) Add(t time.Time, num int) time.Time {
	return t.Add(timeutil.Week * time.Duration(num*w.count))
}

// Truncate to the nearest Sunday that is a multiple of the count starting from 0000-31-12
func (w *weekInterval) Truncate(t time.Time) time.Time {
	// add a day to Sundays to prevent times that would truncate to themselves from going to the previous step
	if t.UTC().Weekday() == time.Sunday {
		t = t.UTC().AddDate(0, 0, 1)
	}

	// truncate to monday using a weekly duration multiple (0001-01-01 was a monday) then subtract a day
	return t.UTC().Truncate(timeutil.Week * time.Duration(w.count)).Add(-timeutil.Day)
}
