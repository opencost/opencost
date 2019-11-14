package costmodel

import (
	"math"
	"sort"
)

type Vector struct {
	Timestamp float64 `json:"timestamp"`
	Value     float64 `json:"value"`
}

// roundTimestamp rounds the given timestamp to the given precision; e.g. a
// timestamp given in seconds, rounded to precision 10, will be rounded
// to the nearest value dividible by 10 (24 goes to 20, but 25 goes to 30).
func roundTimestamp(ts float64, precision float64) float64 {
	return math.Round(ts/precision) * precision
}

// ApplyVectorOp accepts two vectors, synchronizes timestamps, and executes an operation
// on each vector. See VectorJoinOp for details.
func ApplyVectorOp(xvs []*Vector, yvs []*Vector, op VectorJoinOp) []*Vector {
	// round all non-zero timestamps to the nearest 10 second mark
	for _, yv := range yvs {
		if yv.Timestamp != 0 {
			yv.Timestamp = roundTimestamp(yv.Timestamp, 10.0)
		}
	}
	for _, xv := range xvs {
		if xv.Timestamp != 0 {
			xv.Timestamp = roundTimestamp(xv.Timestamp, 10.0)
		}
	}

	// if xvs is empty, return yvs
	if xvs == nil || len(xvs) == 0 {
		return yvs
	}

	// if yvs is empty, return xvs
	if yvs == nil || len(yvs) == 0 {
		return xvs
	}

	// result contains the final vector slice after joining xvs and yvs
	var result []*Vector

	// timestamps stores all timestamps present in both vector slices
	// without duplicates
	var timestamps []float64

	// turn each vector slice into a map of timestamp-to-value so that
	// values at equal timestamps can be lined-up and summed
	xMap := make(map[float64]float64)
	for _, xv := range xvs {
		if xv.Timestamp == 0 {
			continue
		}
		xMap[xv.Timestamp] = xv.Value
		timestamps = append(timestamps, xv.Timestamp)
	}
	yMap := make(map[float64]float64)
	for _, yv := range yvs {
		if yv.Timestamp == 0 {
			continue
		}
		yMap[yv.Timestamp] = yv.Value
		if _, ok := xMap[yv.Timestamp]; !ok {
			// no need to double add, since we'll range over sorted timestamps and check.
			timestamps = append(timestamps, yv.Timestamp)
		}
	}

	// iterate over each timestamp to produce a final op vector slice
	sort.Float64s(timestamps)
	for _, t := range timestamps {
		x, okX := xMap[t]
		y, okY := yMap[t]
		sv := &Vector{Timestamp: t}

		if op(sv, VectorValue(x, okX), VectorValue(y, okY)) {
			result = append(result, sv)
		}
	}

	return result
}

// VectorJoinOp is an operation func that accepts a result vector pointer
// for a specific timestamp and two float64 pointers representing the
// input vectors for that timestamp. x or y inputs can be nil, but not
// both. The op should use x and y values to set the Value on the result
// ptr. If a result could not be generated, the op should return false,
// which will omit the vector for the specific timestamp. Otherwise,
// return true denoting a successful op.
type VectorJoinOp func(result *Vector, x *float64, y *float64) bool

// returns a nil ptr or valid float ptr based on the ok bool
func VectorValue(v float64, ok bool) *float64 {
	if !ok {
		return nil
	}

	return &v
}

// NormalizeVectorByVector produces a version of xvs (a slice of Vectors)
// which has had its timestamps rounded and its values divided by the values
// of the Vectors of yvs, such that yvs is the "unit" Vector slice.
func NormalizeVectorByVector(xvs []*Vector, yvs []*Vector) []*Vector {
	// round all non-zero timestamps to the nearest 10 second mark
	for _, yv := range yvs {
		if yv.Timestamp != 0 {
			yv.Timestamp = roundTimestamp(yv.Timestamp, 10.0)
		}
	}
	for _, xv := range xvs {
		if xv.Timestamp != 0 {
			xv.Timestamp = roundTimestamp(xv.Timestamp, 10.0)
		}
	}

	// if xvs is empty, return yvs
	if xvs == nil || len(xvs) == 0 {
		return yvs
	}

	// if yvs is empty, return xvs
	if yvs == nil || len(yvs) == 0 {
		return xvs
	}

	// sum stores the sum of the vector slices xvs and yvs
	var sum []*Vector

	// timestamps stores all timestamps present in both vector slices
	// without duplicates
	var timestamps []float64

	// turn each vector slice into a map of timestamp-to-value so that
	// values at equal timestamps can be lined-up and summed
	xMap := make(map[float64]float64)
	for _, xv := range xvs {
		if xv.Timestamp == 0 {
			continue
		}
		xMap[xv.Timestamp] = xv.Value
		timestamps = append(timestamps, xv.Timestamp)
	}
	yMap := make(map[float64]float64)
	for _, yv := range yvs {
		if yv.Timestamp == 0 {
			continue
		}
		yMap[yv.Timestamp] = yv.Value
		if _, ok := xMap[yv.Timestamp]; !ok {
			// no need to double add, since we'll range over sorted timestamps and check.
			timestamps = append(timestamps, yv.Timestamp)
		}
	}

	// iterate over each timestamp to produce a final normalized vector slice
	sort.Float64s(timestamps)
	for _, t := range timestamps {
		x, okX := xMap[t]
		y, okY := yMap[t]
		sv := &Vector{Timestamp: t}
		if okX && okY && y != 0 {
			sv.Value = x / y
		} else if okX {
			sv.Value = x
		} else if okY {
			sv.Value = 0
		}
		sum = append(sum, sv)
	}

	return sum
}
