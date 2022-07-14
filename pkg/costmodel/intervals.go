package costmodel

import (
	"sort"
	"time"

	"github.com/opencost/opencost/pkg/kubecost"
)

// IntervalPoint describes a start or end of a window of time
// Currently, this used in PVC-pod relations to detect/calculate
// coefficients for PV cost when a PVC is shared between pods.
type IntervalPoint struct {
	Time      time.Time
	PointType string
	Key       podKey
}

// IntervalPoints describes a slice of IntervalPoint structs
type IntervalPoints []IntervalPoint

// Requisite functions for implementing sort.Sort for
// IntervalPointList
func (ips IntervalPoints) Len() int {
	return len(ips)
}

func (ips IntervalPoints) Less(i, j int) bool {
	if ips[i].Time.Equal(ips[j].Time) {
		return ips[i].PointType == "start" && ips[j].PointType == "end"
	}
	return ips[i].Time.Before(ips[j].Time)
}

func (ips IntervalPoints) Swap(i, j int) {
	ips[i], ips[j] = ips[j], ips[i]
}

// NewIntervalPoint creates and returns a new IntervalPoint instance with given parameters.
func NewIntervalPoint(time time.Time, pointType string, key podKey) IntervalPoint {
	return IntervalPoint{
		Time:      time,
		PointType: pointType,
		Key:       key,
	}
}

// CoefficientComponent is a representitive struct holding two fields which describe an interval
// as part of a single number cost coefficient calculation:
// 1. Proportion: The division of cost based on how many pods were running between those points
// 2. Time: The ratio of the time between those points to the total time that pod was running
type CoefficientComponent struct {
	Proportion float64
	Time       float64
}

// getIntervalPointFromWindows takes a map of podKeys to windows
// and returns a sorted list of IntervalPoints representing the
// starts and ends of all those windows.
func getIntervalPointsFromWindows(windows map[podKey]kubecost.Window) IntervalPoints {

	var intervals IntervalPoints

	for podKey, podInterval := range windows {

		start := NewIntervalPoint(*podInterval.Start(), "start", podKey)
		end := NewIntervalPoint(*podInterval.End(), "end", podKey)

		intervals = append(intervals, []IntervalPoint{start, end}...)

	}

	sort.Sort(intervals)

	return intervals

}

// getPVCCostCoefficients gets a coefficient which represents the scale
// factor that each PVC in a pvcIntervalMap and corresponding slice of
// IntervalPoints intervals uses to calculate a cost for that PVC's PV.
func getPVCCostCoefficients(intervals IntervalPoints, pvcIntervalMap map[podKey]kubecost.Window) map[podKey][]CoefficientComponent {

	pvcCostCoefficientMap := make(map[podKey][]CoefficientComponent)

	// pvcCostCoefficientMap is mutated in this function. The format is
	// such that the individual coefficient components are preserved for
	// testing purposes.

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
				if pvcIntervalMap[key].Duration().Minutes() != 0 {
					pvcCostCoefficientMap[key] = append(
						pvcCostCoefficientMap[key],
						CoefficientComponent{
							Time:       point.Time.Sub(prevPoint.Time).Minutes() / pvcIntervalMap[key].Duration().Minutes(),
							Proportion: 1.0 / float64(len(activeKeys)),
						},
					)
				}
			}
		}

		// If the point was a start, increment and track
		if point.PointType == "start" {
			activeKeys[point.Key] = struct{}{}
		}

		// If the point was an end, decrement and stop tracking
		if point.PointType == "end" {
			delete(activeKeys, point.Key)
		}

	}

	return pvcCostCoefficientMap
}

// getCoefficientFromComponents takes the components of a PVC-pod PV cost coefficient
// determined by getPVCCostCoefficient and gets the resulting single
// floating point coefficient.
func getCoefficientFromComponents(coefficientComponents []CoefficientComponent) float64 {

	coefficient := 0.0

	for i := range coefficientComponents {

		proportion := coefficientComponents[i].Proportion
		time := coefficientComponents[i].Time

		coefficient += proportion * time

	}

	return coefficient
}
