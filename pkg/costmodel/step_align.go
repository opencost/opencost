package costmodel

import (
	"time"

	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

// alignStepStart aligns the first step boundary of a stepped query so that
// step-sized buckets correspond to natural calendar intervals rather than
// arbitrary offsets from the request's start time.
//
// This is currently used only for the weekly step, which is rolled back to
// 00:00 UTC on the preceding Sunday via timeutil.RoundToStartOfWeek. Other
// step durations are returned unchanged.
//
// For example, a request with a window starting mid-week and step=1w would,
// without alignment, produce buckets that do not correspond to a calendar
// week. With alignment, the first bucket begins at the preceding Sunday
// 00:00 UTC, so subsequent bucket boundaries fall on Sunday 00:00 UTC.
func alignStepStart(start time.Time, step time.Duration) time.Time {
	if step == timeutil.Week {
		return timeutil.RoundToStartOfWeek(start)
	}
	return start
}
