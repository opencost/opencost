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
// 00:00 on the preceding Monday. Other step durations are returned unchanged.
//
// For example, a request with a window starting on Tuesday and step=1w would,
// without alignment, produce buckets that run Tuesday-to-Tuesday. With
// alignment, the first bucket begins at the preceding Monday 00:00, which
// matches the calendar week most users expect and avoids an initial bucket
// that only covers a partial week of data.
func alignStepStart(start time.Time, step time.Duration) time.Time {
	if step == timeutil.Week {
		return timeutil.RoundToStartOfWeekMonday(start)
	}
	return start
}
