package kubecost

import (
	"bytes"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"time"
)

const (
	minutesPerDay  = 60 * 24
	minutesPerHour = 60
	hoursPerDay    = 24
)

// RoundBack rounds the given time back to a multiple of the given resolution
// in the given time's timezone.
// e.g. 2020-01-01T12:37:48-0700, 24h = 2020-01-01T00:00:00-0700
func RoundBack(t time.Time, resolution time.Duration) time.Time {
	_, offSec := t.Zone()
	return t.Add(time.Duration(offSec) * time.Second).Truncate(resolution).Add(-time.Duration(offSec) * time.Second)
}

// RoundForward rounds the given time forward to a multiple of the given resolution
// in the given time's timezone.
// e.g. 2020-01-01T12:37:48-0700, 24h = 2020-01-02T00:00:00-0700
func RoundForward(t time.Time, resolution time.Duration) time.Time {
	back := RoundBack(t, resolution)
	if back.Equal(t) {
		// The given time is exactly a multiple of the given resolution
		return t
	}
	return back.Add(resolution)
}

// Window defines a period of time with a start and an end. If either start or
// end are nil it indicates an open time period.
type Window struct {
	start *time.Time
	end   *time.Time
}

// NewWindow creates and returns a new Window instance from the given times
func NewWindow(start, end *time.Time) Window {
	return Window{
		start: start,
		end:   end,
	}
}

// ParseWindowUTC attempts to parse the given string into a valid Window. It
// accepts several formats, returning an error if the given string does not
// match one of the following:
// - named intervals: "today", "yesterday", "week", "month", "lastweek", "lastmonth"
// - durations: "24h", "7d", etc.
// - date ranges: "2020-04-01T00:00:00Z,2020-04-03T00:00:00Z", etc.
// - timestamp ranges: "1586822400,1586908800", etc.
func ParseWindowUTC(window string) (Window, error) {
	return parseWindow(window, time.Now().UTC())
}

// ParseWindowWithOffsetString parses the given window string within the context of
// the timezone defined by the UTC offset string of format -07:00, +01:30, etc.
func ParseWindowWithOffsetString(window string, offset string) (Window, error) {
	if offset == "UTC" || offset == "" {
		return ParseWindowUTC(window)
	}

	regex := regexp.MustCompile(`^(\+|-)(\d\d):(\d\d)$`)
	match := regex.FindStringSubmatch(offset)
	if match == nil {
		return Window{}, fmt.Errorf("illegal UTC offset: '%s'; should be of form '-07:00'", offset)
	}

	sig := 1
	if match[1] == "-" {
		sig = -1
	}

	hrs64, _ := strconv.ParseInt(match[2], 10, 64)
	hrs := sig * int(hrs64)

	mins64, _ := strconv.ParseInt(match[3], 10, 64)
	mins := sig * int(mins64)

	loc := time.FixedZone(fmt.Sprintf("UTC%s", offset), (hrs*60*60)+(mins*60))
	now := time.Now().In(loc)
	return parseWindow(window, now)
}

// ParseWindowWithOffset parses the given window string within the context of
// the timezone defined by the UTC offset.
func ParseWindowWithOffset(window string, offset time.Duration) (Window, error) {
	loc := time.FixedZone("", int(offset.Seconds()))
	now := time.Now().In(loc)
	return parseWindow(window, now)
}

// parseWindow generalizes the parsing of window strings, relative to a given
// moment in time, defined as "now".
func parseWindow(window string, now time.Time) (Window, error) {
	// compute UTC offset in terms of minutes
	offHr := now.UTC().Hour() - now.Hour()
	offMin := (now.UTC().Minute() - now.Minute()) + (offHr * 60)
	offset := time.Duration(offMin) * time.Minute

	if window == "today" {
		start := now
		start = start.Truncate(time.Hour * 24)
		start = start.Add(offset)

		end := start.Add(time.Hour * 24)

		return NewWindow(&start, &end), nil
	}

	if window == "yesterday" {
		start := now
		start = start.Truncate(time.Hour * 24)
		start = start.Add(offset)
		start = start.Add(time.Hour * -24)

		end := start.Add(time.Hour * 24)

		return NewWindow(&start, &end), nil
	}

	if window == "week" {
		// now
		start := now
		// 00:00 today, accounting for timezone offset
		start = start.Truncate(time.Hour * 24)
		start = start.Add(offset)
		// 00:00 Sunday of the current week
		start = start.Add(-24 * time.Hour * time.Duration(start.Weekday()))

		end := now

		return NewWindow(&start, &end), nil
	}

	if window == "lastweek" {
		// now
		start := now
		// 00:00 today, accounting for timezone offset
		start = start.Truncate(time.Hour * 24)
		start = start.Add(offset)
		// 00:00 Sunday of last week
		start = start.Add(-24 * time.Hour * time.Duration(start.Weekday()+7))

		end := start.Add(7 * 24 * time.Hour)

		return NewWindow(&start, &end), nil
	}

	if window == "month" {
		// now
		start := now
		// 00:00 today, accounting for timezone offset
		start = start.Truncate(time.Hour * 24)
		start = start.Add(offset)
		// 00:00 1st of this month
		start = start.Add(-24 * time.Hour * time.Duration(start.Day()-1))

		end := now

		return NewWindow(&start, &end), nil
	}

	if window == "month" {
		// now
		start := now
		// 00:00 today, accounting for timezone offset
		start = start.Truncate(time.Hour * 24)
		start = start.Add(offset)
		// 00:00 1st of this month
		start = start.Add(-24 * time.Hour * time.Duration(start.Day()-1))

		end := now

		return NewWindow(&start, &end), nil
	}

	if window == "lastmonth" {
		// now
		end := now
		// 00:00 today, accounting for timezone offset
		end = end.Truncate(time.Hour * 24)
		end = end.Add(offset)
		// 00:00 1st of this month
		end = end.Add(-24 * time.Hour * time.Duration(end.Day()-1))

		// 00:00 last day of last month
		start := end.Add(-24 * time.Hour)
		// 00:00 1st of last month
		start = start.Add(-24 * time.Hour * time.Duration(start.Day()-1))

		return NewWindow(&start, &end), nil
	}

	// Match duration strings; e.g. "45m", "24h", "7d"
	regex := regexp.MustCompile(`^(\d+)(m|h|d)$`)
	match := regex.FindStringSubmatch(window)
	if match != nil {
		dur := time.Minute
		if match[2] == "h" {
			dur = time.Hour
		}
		if match[2] == "d" {
			dur = 24 * time.Hour
		}

		num, _ := strconv.ParseInt(match[1], 10, 64)

		end := now
		start := end.Add(-time.Duration(num) * dur)

		return NewWindow(&start, &end), nil
	}

	// Match duration strings with offset; e.g. "45m offset 15m", etc.
	regex = regexp.MustCompile(`^(\d+)(m|h|d) offset (\d+)(m|h|d)$`)
	match = regex.FindStringSubmatch(window)
	if match != nil {
		end := now

		offUnit := time.Minute
		if match[4] == "h" {
			offUnit = time.Hour
		}
		if match[4] == "d" {
			offUnit = 24 * time.Hour
		}

		offNum, _ := strconv.ParseInt(match[3], 10, 64)

		end = end.Add(-time.Duration(offNum) * offUnit)

		durUnit := time.Minute
		if match[2] == "h" {
			durUnit = time.Hour
		}
		if match[2] == "d" {
			durUnit = 24 * time.Hour
		}

		durNum, _ := strconv.ParseInt(match[1], 10, 64)

		start := end.Add(-time.Duration(durNum) * durUnit)

		return NewWindow(&start, &end), nil
	}

	// Match timestamp pairs, e.g. "1586822400,1586908800" or "1586822400-1586908800"
	regex = regexp.MustCompile(`^(\d+)[,|-](\d+)$`)
	match = regex.FindStringSubmatch(window)
	if match != nil {
		s, _ := strconv.ParseInt(match[1], 10, 64)
		e, _ := strconv.ParseInt(match[2], 10, 64)
		start := time.Unix(s, 0)
		end := time.Unix(e, 0)
		return NewWindow(&start, &end), nil
	}

	// Match RFC3339 pairs, e.g. "2020-04-01T00:00:00Z,2020-04-03T00:00:00Z"
	rfc3339 := `\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\dZ`
	regex = regexp.MustCompile(fmt.Sprintf(`(%s),(%s)`, rfc3339, rfc3339))
	match = regex.FindStringSubmatch(window)
	if match != nil {
		start, _ := time.Parse(time.RFC3339, match[1])
		end, _ := time.Parse(time.RFC3339, match[2])
		return NewWindow(&start, &end), nil
	}

	return Window{nil, nil}, fmt.Errorf("illegal window: %s", window)
}

// ApproximatelyEqual returns true if the start and end times of the two windows,
// respectively, are within the given threshold of each other.
func (w Window) ApproximatelyEqual(that Window, threshold time.Duration) bool {
	return approxEqual(w.start, that.start, threshold) && approxEqual(w.end, that.end, threshold)
}

func approxEqual(x *time.Time, y *time.Time, threshold time.Duration) bool {
	// both times are nil, so they are equal
	if x == nil && y == nil {
		return true
	}

	// one time is nil, but the other is not, so they are not equal
	if x == nil || y == nil {
		return false
	}

	// neither time is nil, so they are approximately close if their times are
	// within the given threshold
	delta := math.Abs((*x).Sub(*y).Seconds())
	return delta < threshold.Seconds()
}

func (w Window) Clone() Window {
	var start, end *time.Time
	var s, e time.Time

	if w.start != nil {
		s = *w.start
		start = &s
	}

	if w.end != nil {
		e = *w.end
		end = &e
	}

	return NewWindow(start, end)
}

func (w Window) Contains(t time.Time) bool {
	if w.start != nil && t.Before(*w.start) {
		return false
	}

	if w.end != nil && t.After(*w.end) {
		return false
	}

	return true
}

func (w Window) Duration() time.Duration {
	if w.start != nil && w.end != nil {
		return w.end.Sub(*w.start)
	}

	return 0
}

func (w Window) End() *time.Time {
	return w.end
}

func (w Window) Equal(that Window) bool {
	if w.start != nil && that.start != nil && !w.start.Equal(*that.start) {
		// starts are not nil, but not equal
		return false
	}

	if w.end != nil && that.end != nil && !w.end.Equal(*that.end) {
		// ends are not nil, but not equal
		return false
	}

	if (w.start == nil && that.start != nil) || (w.start != nil && that.start == nil) {
		// one start is nil, the other is not
		return false
	}

	if (w.end == nil && that.end != nil) || (w.end != nil && that.end == nil) {
		// one end is nil, the other is not
		return false
	}

	// either both starts are nil, or they match; likewise for the ends
	return true
}

func (w Window) ExpandStart(start time.Time) Window {
	if w.start == nil || start.Before(*w.start) {
		w.start = &start
	}
	return w
}

func (w Window) ExpandEnd(end time.Time) Window {
	if w.end == nil || end.After(*w.end) {
		w.end = &end
	}
	return w
}

func (w Window) Expand(that Window) Window {
	return w.ExpandStart(*that.start).ExpandEnd(*that.end)
}

func (w Window) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString("{")
	buffer.WriteString(fmt.Sprintf("\"start\":\"%s\",", w.start.Format("2006-01-02T15:04:05-0700")))
	buffer.WriteString(fmt.Sprintf("\"end\":\"%s\"", w.end.Format("2006-01-02T15:04:05-0700")))
	buffer.WriteString("}")
	return buffer.Bytes(), nil
}

func (w Window) Minutes() float64 {
	if w.start == nil || w.end == nil {
		return math.Inf(1)
	}

	return w.end.Sub(*w.start).Minutes()
}

// Shift adds the given duration to both the start and end times of the window
func (w Window) Shift(dur time.Duration) Window {
	if w.start != nil {
		s := w.start.Add(dur)
		w.start = &s
	}

	if w.end != nil {
		e := w.end.Add(dur)
		w.end = &e
	}

	return w
}

func (w Window) Start() *time.Time {
	return w.start
}

func (w Window) String() string {
	if w.start == nil && w.end == nil {
		return "[nil, nil)"
	}
	if w.start == nil {
		return fmt.Sprintf("[nil, %s)", w.end.Format("2006-01-02T15:04:05-0700"))
	}
	if w.end == nil {
		return fmt.Sprintf("[%s, nil)", w.start.Format("2006-01-02T15:04:05-0700"))
	}
	return fmt.Sprintf("[%s, %s)", w.start.Format("2006-01-02T15:04:05-0700"), w.end.Format("2006-01-02T15:04:05-0700"))
}

// ToDurationOffset returns formatted strings representing the duration and
// offset of the window in terms of minutes; e.g. ("30m", "1m")
func (w Window) ToDurationOffset() (string, string) {
	durMins := int(w.Duration().Minutes())

	offStr := ""
	if w.End() != nil {
		offMins := int(time.Now().Sub(*w.End()).Minutes())
		if offMins > 1 {
			offStr = fmt.Sprintf("%dm", int(offMins))
		} else if offMins < -1 {
			durMins += offMins
		}
	}

	// default to formatting in terms of minutes
	durStr := fmt.Sprintf("%dm", durMins)
	if (durMins >= minutesPerDay) && (durMins%minutesPerDay == 0) {
		// convert to days
		durStr = fmt.Sprintf("%dd", durMins/minutesPerDay)
	} else if (durMins >= minutesPerHour) && (durMins%minutesPerHour == 0) {
		// convert to hours
		durStr = fmt.Sprintf("%dh", durMins/minutesPerHour)
	}

	return durStr, offStr
}

type BoundaryError struct {
	Requested Window
	Supported Window
	Message   string
}

func NewBoundaryError(req, sup Window, msg string) *BoundaryError {
	return &BoundaryError{
		Requested: req,
		Supported: sup,
		Message:   msg,
	}
}

func (be *BoundaryError) Error() string {
	if be == nil {
		return "<nil>"
	}

	return fmt.Sprintf("boundary error: requested %s; supported %s: %s", be.Requested, be.Supported, be.Message)
}
