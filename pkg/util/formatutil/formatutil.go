package formatutil

import "math"

func Float64ToResponse(f float64) *float64 {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return nil
	}

	return &f
}
