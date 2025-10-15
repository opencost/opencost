package kubemodel

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/model/pb"
)

// ============================================================================
// Window Resolution Conversions
// ============================================================================
//
// Time windows in the kubemodel package represent discrete time periods for
// aggregating cluster metrics. Each window has a resolution that determines
// both the duration and the granularity of the data.
//
// Supported Resolutions:
//   - 10m: 10-minute windows for high-frequency monitoring
//   - 1h:  1-hour windows for standard operational metrics
//   - 1d:  1-day windows for daily cost analysis and reporting
//
// These functions convert between Go time.Duration values and protobuf
// Resolution enums, ensuring that only supported window sizes are used
// throughout the system.

// DurationToResolution converts a time.Duration to a protobuf Resolution enum.
//
// This function validates that the requested window duration matches one of
// the supported resolutions. Using standardized window sizes ensures:
//   - Consistent aggregation across different data sources
//   - Predictable query performance characteristics
//   - Alignment with underlying metric retention policies
//   - Compatibility with cost aggregation algorithms
//
// Supported durations:
//   - 10 minutes → RESOLUTION_10M
//   - 1 hour     → RESOLUTION_1H
//   - 24 hours   → RESOLUTION_1D
//
// Returns an error if the duration doesn't exactly match a supported resolution.
// This strict validation prevents accidental use of arbitrary time windows that
// could lead to inconsistent or incomplete data.
//
// Example usage:
//
//	start := time.Now().Add(-1 * time.Hour)
//	end := time.Now()
//	duration := end.Sub(start)
//	resolution, err := DurationToResolution(duration)
//	if err != nil {
//	    // Handle unsupported window size
//	}
func DurationToResolution(d time.Duration) (pb.Resolution, error) {
	switch d {
	case 10 * time.Minute:
		return pb.Resolution_RESOLUTION_10M, nil
	case time.Hour:
		return pb.Resolution_RESOLUTION_1H, nil
	case 24 * time.Hour:
		return pb.Resolution_RESOLUTION_1D, nil
	default:
		return pb.Resolution_RESOLUTION_10M, fmt.Errorf("kubemodel: unsupported window duration %s (must be 10m, 1h, or 1d)", d)
	}
}

// ResolutionToDuration converts a protobuf Resolution enum to a time.Duration.
//
// This is the inverse operation of DurationToResolution, used when you need
// to perform time arithmetic or validation based on a resolution value stored
// in a protobuf message.
//
// Use cases:
//   - Calculating the end time given a start time and resolution
//   - Validating that a time range matches the declared resolution
//   - Computing the number of windows in a larger time range
//   - Determining appropriate query timeouts based on window size
//
// Supported resolutions:
//   - RESOLUTION_10M → 10 minutes
//   - RESOLUTION_1H  → 1 hour
//   - RESOLUTION_1D  → 24 hours
//
// Returns an error if the resolution enum is unrecognized or RESOLUTION_UNSPECIFIED.
// This protects against processing data with unknown or invalid resolution values.
//
// Example usage:
//
//	duration, err := ResolutionToDuration(window.Resolution)
//	if err != nil {
//	    // Handle invalid resolution
//	}
//	expectedEnd := window.Start.AsTime().Add(duration)
func ResolutionToDuration(res pb.Resolution) (time.Duration, error) {
	switch res {
	case pb.Resolution_RESOLUTION_10M:
		return 10 * time.Minute, nil
	case pb.Resolution_RESOLUTION_1H:
		return time.Hour, nil
	case pb.Resolution_RESOLUTION_1D:
		return 24 * time.Hour, nil
	default:
		return 0, fmt.Errorf("kubemodel: unsupported window resolution %v (must be RESOLUTION_10M, RESOLUTION_1H, or RESOLUTION_1D)", res)
	}
}
