package model

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/model/pb"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

// ConvertWindow validates and converts a protobuf window to a closed opencost.Window or returns an error
func ConvertWindow(window *pb.Window) (opencost.Window, error) {
	if window == nil {
		return opencost.Window{}, fmt.Errorf("cannot convert nil window")
	}
	var res time.Duration
	switch window.Resolution {
	case pb.Resolution_RESOLUTION_1D:
		res = timeutil.Day
	case pb.Resolution_RESOLUTION_1H:
		res = time.Hour
	case pb.Resolution_RESOLUTION_10M:
		res = time.Minute * 10
	default:
		return opencost.Window{}, fmt.Errorf("invalid window resolution %v", window.Resolution)
	}

	start := window.Start.AsTime().UTC()
	if !start.Equal(start.Truncate(res)) {
		return opencost.Window{}, fmt.Errorf("invalid start time for resolution '%s': %s", window.Resolution, start.Format(time.RFC3339))
	}
	win := opencost.NewClosedWindow(start, start.Add(res))
	return win, nil
}
