package costmodel

import (
	"sort"
	"time"

	"github.com/kubecost/cost-model/pkg/kubecost"
	//"k8s.io/klog"
)

// IntervalPoint describes a start or end of a window of time
// Currently, this used in PVC-pod relations and is used to
// detect/calculate coefficients for PVCs shared between pods.
type IntervalPoint struct {
	Time      time.Time
	PointType string
	Key       podKey
}

func NewIntervalPoint(time time.Time, pointType string, key podKey) IntervalPoint {
	return IntervalPoint{
		Time:      time,
		PointType: pointType,
		Key:       key,
	}
}

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

func sortIntervalPoints(intervals []IntervalPoint) {
	sort.Slice(intervals, func(i, j int) bool {
		if intervals[i].Time.Equal(intervals[j].Time) {
			return intervals[i].PointType == "start" && intervals[j].PointType == "end"
		}
		return intervals[i].Time.Before(intervals[j].Time)
	})
}

func getPVCCostCoefficients(intervals []IntervalPoint, pvcIntervalMap map[podKey]kubecost.Window, pvcCostCoefficientMap map[podKey][][]float64) {

	var activePods float64

	activeKeys := make(map[podKey]struct{})

	for i := 1; i < len(intervals); i++ {

		point := intervals[i]
		prevPoint := intervals[i-1]

		if i == 1 {
			activePods += 1
			activeKeys[prevPoint.Key] = struct{}{}
		}

		if !point.Time.Equal(prevPoint.Time) {
			for key := range activeKeys {
				pvcCostCoefficientMap[key] = append(
					pvcCostCoefficientMap[key],
					[]float64{1.0 / activePods, point.Time.Sub(prevPoint.Time).Minutes() / pvcIntervalMap[key].Duration().Minutes()},
				)
			}
		}

		if point.PointType == "start" {
			activePods += 1
			activeKeys[point.Key] = struct{}{}
		}

		if point.PointType == "end" {
			activePods -= 1
			delete(activeKeys, point.Key)
		}

	}
}

func getCoefficient(coefficientComponents [][]float64) float64 {

	coefficient := 0.0

	for i := range coefficientComponents {

		cost := coefficientComponents[i][0]
		duration := coefficientComponents[i][1]

		coefficient += cost * duration

	}

	return coefficient
}
