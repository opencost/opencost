package costmodel

import (
	"sort"
	"time"

	"github.com/kubecost/cost-model/pkg/kubecost"
)

// IntervalPoint describes a start or end of a window of time
// Currently, this used in PVC-pod relations to detect/calculate
// coefficients for PV cost when a PVC is shared between pods.
type IntervalPoint struct {
	Time      time.Time
	PointType string
	Key       podKey
}

// CostCoefficient is a representitive struct holding two fields which describe an interval:
// 1. Proportion: The division of cost based on how many pods were running between those points
// 2. Time: The ratio of the time between those points to the total time that pod was running
type CostCoefficient struct {
	Proportion float64
	Time       float64
}

// NewIntervalPoint creates and returns a new IntervalPoint instance with given parameters.
func NewIntervalPoint(time time.Time, pointType string, key podKey) IntervalPoint {
	return IntervalPoint{
		Time:      time,
		PointType: pointType,
		Key:       key,
	}
}

// getIntervalPointFromWindows takes a map of podKeys to windows
// and returns a sorted list of IntervalPoints representing the
// starts and ends of all those windows.
func getIntervalPointsFromWindows(windows map[podKey]kubecost.Window) []IntervalPoint {

	var intervals []IntervalPoint

	for podKey, podInterval := range windows {

		start := NewIntervalPoint(*podInterval.Start(), "start", podKey)
		end := NewIntervalPoint(*podInterval.End(), "end", podKey)

		intervals = append(intervals, []IntervalPoint{start, end}...)

	}

	sortIntervalPoints(intervals)

	return intervals

}

// sortIntervalPoints sorts a list of IntervalPoints from earliest
// to latest IntervalPoint.Time. In the case that two IntervalPoints
// have the same time, the point with Type "start" is treated as coming
// before the "end".
func sortIntervalPoints(intervals []IntervalPoint) {
	sort.Slice(intervals, func(i, j int) bool {
		if intervals[i].Time.Equal(intervals[j].Time) {
			return intervals[i].PointType == "start" && intervals[j].PointType == "end"
		}
		return intervals[i].Time.Before(intervals[j].Time)
	})
}

// getPVCCostCoefficients gets a coefficient which represents the scale
// factor that each PVC in a pvcIntervalMap and corresponding slice of
// IntervalPoints intervals uses to calculate a cost for that PVC's PV.
func getPVCCostCoefficients(intervals []IntervalPoint, pvcIntervalMap map[podKey]kubecost.Window) map[podKey][][]float64 {

	pvcCostCoefficientMap := make(map[podKey][][]float64)

	// pvcCostCoefficientMap is mutated in this function. The format is
	// such that the individual coefficient components are preserved for
	// testing purposes.

	activePods := 1.0

	activeKeys := map[podKey]struct{}{
		intervals[0].Key: struct{}{},
	}

	// For each interval i.e. for any time a pod-PVC relation ends or starts...
	for i := 1; i < len(intervals); i++ {

		// intervals will always have at least two IntervalPoints (one start/end)
		point := intervals[i]
		prevPoint := intervals[i-1]

		// If the current point happens at a later time than the previous point
		if !point.Time.Equal(prevPoint.Time) {
			for key := range activeKeys {
				pvcCostCoefficientMap[key] = append(
					pvcCostCoefficientMap[key],
					[]float64{1.0 / activePods, point.Time.Sub(prevPoint.Time).Minutes() / pvcIntervalMap[key].Duration().Minutes()},
				)
			}
		}

		// If the point was a start, increment and track
		if point.PointType == "start" {
			activePods += 1
			activeKeys[point.Key] = struct{}{}
		}

		// If the point was an end, decrement and stop tracking
		if point.PointType == "end" {
			activePods -= 1
			delete(activeKeys, point.Key)
		}

	}

	return pvcCostCoefficientMap
}

// getCoefficient takes the components of a PVC-pod PV cost coefficient
// determined by getPVCCostCoefficient and gets the resulting single
// floating point coefficient.
func getCoefficient(coefficientComponents [][]float64) float64 {

	coefficient := 0.0

	for i := range coefficientComponents {

		cost := coefficientComponents[i][0]
		duration := coefficientComponents[i][1]

		coefficient += cost * duration

	}

	return coefficient
}
