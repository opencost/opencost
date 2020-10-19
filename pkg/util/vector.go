package util

import (
	"math"
	"sort"

	"github.com/kubecost/cost-model/pkg/util/memory"
)

type Vector struct {
	Timestamp float64 `json:"timestamp"`
	Value     float64 `json:"value"`
}

const MapPoolSize = 4

// VectorSlice is an alias used to sort vectors
type VectorSlice []*Vector

func (p VectorSlice) Len() int           { return len(p) }
func (p VectorSlice) Less(i, j int) bool { return p[i].Timestamp < p[j].Timestamp }
func (p VectorSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

var (
	mapPool      memory.VectorMapPool      = memory.NewFlexibleMapPool(MapPoolSize)
	multiMapPool memory.MultiVectorMapPool = memory.NewFlexibleMultiMapPool(MapPoolSize)
	floatPool    *memory.FloatPool         = memory.NewFloatPool(100)
)

// roundTimestamp rounds the given timestamp to the given precision; e.g. a
// timestamp given in seconds, rounded to precision 10, will be rounded
// to the nearest value dividible by 10 (24 goes to 20, but 25 goes to 30).
func roundTimestamp(ts float64, precision float64) float64 {
	return math.Round(ts/precision) * precision
}

// Makes a reasonable guess at capacity for vector join
func capacityFor(xvs []*Vector, yvs []*Vector) int {
	x := len(xvs)
	y := len(yvs)

	if x >= y {
		return x + (y / 4)
	}

	return y + (x / 4)
}

// ApplyVectorOp accepts two vectors, synchronizes timestamps, and executes an operation
// on each vector. See VectorJoinOp for details.
func ApplyVectorOp(xvs []*Vector, yvs []*Vector, op VectorJoinOp) []*Vector {
	// if xvs is empty, return yvs
	if xvs == nil || len(xvs) == 0 {
		return yvs
	}

	// if yvs is empty, return xvs
	if yvs == nil || len(yvs) == 0 {
		return xvs
	}

	// timestamps contains the vector slice after joining xvs and yvs
	var timestamps []*Vector

	// turn each vector slice into a map of timestamp-to-value so that
	// values at equal timestamps can be lined-up and summed
	xMap := mapPool.Get()
	defer mapPool.Put(xMap)

	for _, xv := range xvs {
		if xv.Timestamp == 0 {
			continue
		}

		// round all non-zero timestamps to the nearest 10 second mark
		xv.Timestamp = roundTimestamp(xv.Timestamp, 10.0)

		xMap[uint64(xv.Timestamp)] = xv.Value
		timestamps = append(timestamps, &Vector{
			Timestamp: xv.Timestamp,
		})
	}

	yMap := mapPool.Get()
	defer mapPool.Put(yMap)

	for _, yv := range yvs {
		if yv.Timestamp == 0 {
			continue
		}

		// round all non-zero timestamps to the nearest 10 second mark
		yv.Timestamp = roundTimestamp(yv.Timestamp, 10.0)

		yMap[uint64(yv.Timestamp)] = yv.Value
		if _, ok := xMap[uint64(yv.Timestamp)]; !ok {
			// no need to double add, since we'll range over sorted timestamps and check.
			timestamps = append(timestamps, &Vector{
				Timestamp: yv.Timestamp,
			})
		}
	}

	// iterate over each timestamp to produce a final op vector slice
	// reuse the existing slice to reduce allocations
	result := timestamps[:0]
	for _, sv := range timestamps {
		x, okX := xMap[uint64(sv.Timestamp)]
		y, okY := yMap[uint64(sv.Timestamp)]

		if op(sv, VectorValue(x, okX), VectorValue(y, okY)) {
			result = append(result, sv)
		}
	}

	// nil out remaining vectors in timestamps to GC
	for i := len(result); i < len(timestamps); i++ {
		timestamps[i] = nil
	}

	sort.Sort(VectorSlice(result))

	return result
}

// ApplyMultiVectorOp accepts M vectors, synchronizes timestamps (N), and executes an operation
// on each vector's values for the collection timestamps. Providing M=5 Vectors, assuming N=8 unique
// timestamps across all Vectors, ApplyMultiVectorOp will construct a MxN (5x8) matrix, and call the
// provided MultiVectorJoinOp for each column (timestamp) passing the values at that timestamp for each
// of the 5 Vectors.
//
//                ++-------------------------------------------------------++
//                ||                    TimeStamps                         ||
//     ++---------++-----++-----++-----++-----++-----++-----++-----++------++
//     || Vectors || 599 || 659 || 719 || 779 || 839 || 899 || 959 || 1019 ||
//     ++---------++-----++-----++-----++-----++-----++-----++-----++------++
//     ||    A    || 2.1 || nil || nil || 3.2 || nil || 1.4 || nil || nil  ||
//     ||    B    || nil || 1.1 || 5.6 || 2.1 || nil || nil || 1.1 || 3.2  ||
//     ||    C    || nil || 3.5 || nil || nil || nil || 8.3 || 5.2 || nil  ||
//     ||    D    || 6.6 || nil || nil || 1.0 || 9.7 || nil || nil || nil  ||
//     ||    E    || nil || 8.2 || 7.2 || 4.4 || 5.0 || 3.9 || nil || nil  ||
//     ++---------++-----++-----++-----++-----++-----++-----++-----++------++
//
// For each timestamp (starting with the earliest), the MultiVectorJoinOp will get called passing in a
// result *Vector with the timestamp set, and the in-order slice for that column (representing values at
// that timestamp for Vectors A-E). For example, the first call to MultiVectorJoinOp will contain the following:
//
//     op(&Vector{ timestamp: 599 }, []*float64{ 2.1, nil, nil, 6.6, nil })
//
// The MultiVectorJoinOp's job will be to set the *Vector's Value field to the result of an operation on those values.
//
// It's important to note that it is completely up to the developer of the MultiVectorJoinOp to intelligently
// implement the operation aware of the semantics of this function. Associative operations are safe (+, *) where
// the ordering doesn't matter. Associativity is not a hard limitation, but one must use caution where ordering
// matters. For instance, you could have a normalization operation which adds values at indices 0 through len(values)-2,
// then divide the result by the value at index len(values)-1. However, this operation would require passing the Vectors
// into ApplyMultiVectorOp() in the order required for achieve the expected result. These "special" rules with ordering
// should be formalized using a utility/helper method that enforces the rules. ie:
//
//     func NormalizeMulti(normalizeBy []*Vector, joinVecs ...[]*Vector) []*Vector {
//         // Sums all the values except for the last, divides by last index
//         normalizeOp := func(result *util.Vector, values []*float64) bool {
//             var result *float64
//             for i, v := range values {
//                 if i == len(values)-1 {
//                     if result != nil && v != nil {
//                         sum := *result
//                         normalizeBy := *v
//                         if sum == 0 || normalizeBy == 0 {
//                             result.Value = 0
//                         } else {
//                             result.Value = sum / normalizeBy
//                         }
//                     }
//                     continue
//                 }
//                 if v != nil {
//                     if result != nil {
//                         *result += *v
//                     } else {
//                         xv := *v
//                         result = &xv
//                     }
//                 }
//             }
//             return true
//         }
//
//         // create a single input based on the concrete op
//         joinVecs = append(joinVecs, normalizeBy)
//         return ApplyMultiVectorOp(normalizeOp, joinVecs)
//     }
//
// The above function could also be implemented by an associative addVectors() MultiVectorOp
// followed by a binary NormalizeVectorByVector()
func ApplyMultiVectorOp(op MultiVectorJoinOp, vecs ...[]*Vector) []*Vector {
	total := len(vecs)

	//m := multiMapPool.Get()
	//defer multiMapPool.Put(m)
	m := make(map[uint64][]*float64)
	for index, vs := range vecs {
		for _, v := range vs {
			val := v.Value
			ts := roundTimestamp(v.Timestamp, 10.0)
			uts := uint64(ts)

			if _, ok := m[uts]; !ok {
				//m[uts] = make([]*float64, total)
				m[uts] = floatPool.Make(total)
			}

			m[uts][index] = &val
		}
	}

	var results []*Vector
	for k, v := range m {
		rv := &Vector{
			Timestamp: float64(k),
		}

		if op(rv, v) {
			results = append(results, rv)
		}
		floatPool.Return(v)
	}

	sort.Sort(VectorSlice(results))

	return results
}

// VectorJoinOp is an operation func that accepts a result vector pointer
// for a specific timestamp and two float64 pointers representing the
// input vectors for that timestamp. x or y inputs can be nil, but not
// both. The op should use x and y values to set the Value on the result
// ptr. If a result could not be generated, the op should return false,
// which will omit the vector for the specific timestamp. Otherwise,
// return true denoting a successful op.
type VectorJoinOp func(result *Vector, x *float64, y *float64) bool

// MultiVectorJoinOp is similar to VectorJoinOp, however instead of accepting
// exactly 2 values (x and y respectively), you get an ordered slice of potential
// float64 values. Nil is a potential value for some indices of the slice, but
// _at least one_ index will contain a non-nil value.
type MultiVectorJoinOp func(result *Vector, values []*float64) bool

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
	normalizeMultiOp := func(result *Vector, values []*float64) bool {
		x := values[0]
		y := values[1]

		if x != nil && y != nil && *y != 0 {
			result.Value = *x / *y
		} else if x != nil {
			result.Value = *x
		} else if y != nil {
			result.Value = 0
		}

		return true
	}

	//return ApplyVectorOp(xvs, yvs, normalizeOp)
	return ApplyMultiVectorOp(normalizeMultiOp, xvs, yvs)
}
