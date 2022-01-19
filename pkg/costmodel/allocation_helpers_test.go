package costmodel

import (
	"reflect"
	"testing"
	"time"

	"github.com/kubecost/cost-model/pkg/kubecost"
)

func TestGetIntervalPointsFromWindows(t *testing.T) {
	cases := []struct {
		name           string
		pvcIntervalMap map[podKey]kubecost.Window
		expected       []IntervalPoint
	}{
		{
			name: "four pods w/ various overlaps",
			pvcIntervalMap: map[podKey]kubecost.Window{
				// Pod running from 8 am to 9 am
				podKey{
					Pod: "Pod1",
				}: kubecost.Window(kubecost.NewClosedWindow(
					time.Date(2021, 2, 19, 8, 0, 0, 0, time.UTC),
					time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC),
				)),
				// Pod running from 8:30 am to 9 am
				podKey{
					Pod: "Pod2",
				}: kubecost.Window(kubecost.NewClosedWindow(
					time.Date(2021, 2, 19, 8, 30, 0, 0, time.UTC),
					time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC),
				)),
				// Pod running from 8:45 am to 9 am
				podKey{
					Pod: "Pod3",
				}: kubecost.Window(kubecost.NewClosedWindow(
					time.Date(2021, 2, 19, 8, 45, 0, 0, time.UTC),
					time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC),
				)),
				// Pod running from 8 am to 8:15 am
				podKey{
					Pod: "Pod4",
				}: kubecost.Window(kubecost.NewClosedWindow(
					time.Date(2021, 2, 19, 8, 0, 0, 0, time.UTC),
					time.Date(2021, 2, 19, 8, 15, 0, 0, time.UTC),
				)),
			},
			expected: []IntervalPoint{
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 0, 0, 0, time.UTC), "start", podKey{Pod: "Pod1"}),
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 0, 0, 0, time.UTC), "start", podKey{Pod: "Pod4"}),
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 15, 0, 0, time.UTC), "end", podKey{Pod: "Pod4"}),
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 30, 0, 0, time.UTC), "start", podKey{Pod: "Pod2"}),
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 45, 0, 0, time.UTC), "start", podKey{Pod: "Pod3"}),
				NewIntervalPoint(time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC), "end", podKey{Pod: "Pod2"}),
				NewIntervalPoint(time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC), "end", podKey{Pod: "Pod3"}),
				NewIntervalPoint(time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC), "end", podKey{Pod: "Pod1"}),
			},
		},
		{
			name: "two pods no overlap",
			pvcIntervalMap: map[podKey]kubecost.Window{
				// Pod running from 8 am to 8:30 am
				podKey{
					Pod: "Pod1",
				}: kubecost.Window(kubecost.NewClosedWindow(
					time.Date(2021, 2, 19, 8, 0, 0, 0, time.UTC),
					time.Date(2021, 2, 19, 8, 30, 0, 0, time.UTC),
				)),
				// Pod running from 8:30 am to 9 am
				podKey{
					Pod: "Pod2",
				}: kubecost.Window(kubecost.NewClosedWindow(
					time.Date(2021, 2, 19, 8, 30, 0, 0, time.UTC),
					time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC),
				)),
			},
			expected: []IntervalPoint{
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 0, 0, 0, time.UTC), "start", podKey{Pod: "Pod1"}),
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 30, 0, 0, time.UTC), "start", podKey{Pod: "Pod2"}),
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 30, 0, 0, time.UTC), "end", podKey{Pod: "Pod1"}),
				NewIntervalPoint(time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC), "end", podKey{Pod: "Pod2"}),
			},
		},
		{
			name: "two pods total overlap",
			pvcIntervalMap: map[podKey]kubecost.Window{
				// Pod running from 8:30 am to 9 am
				podKey{
					Pod: "Pod1",
				}: kubecost.Window(kubecost.NewClosedWindow(
					time.Date(2021, 2, 19, 8, 30, 0, 0, time.UTC),
					time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC),
				)),
				// Pod running from 8:30 am to 9 am
				podKey{
					Pod: "Pod2",
				}: kubecost.Window(kubecost.NewClosedWindow(
					time.Date(2021, 2, 19, 8, 30, 0, 0, time.UTC),
					time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC),
				)),
			},
			expected: []IntervalPoint{
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 30, 0, 0, time.UTC), "start", podKey{Pod: "Pod1"}),
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 30, 0, 0, time.UTC), "start", podKey{Pod: "Pod2"}),
				NewIntervalPoint(time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC), "end", podKey{Pod: "Pod1"}),
				NewIntervalPoint(time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC), "end", podKey{Pod: "Pod2"}),
			},
		},
		{
			name: "one pod",
			pvcIntervalMap: map[podKey]kubecost.Window{
				// Pod running from 8 am to 9 am
				podKey{
					Pod: "Pod1",
				}: kubecost.Window(kubecost.NewClosedWindow(
					time.Date(2021, 2, 19, 8, 0, 0, 0, time.UTC),
					time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC),
				)),
			},
			expected: []IntervalPoint{
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 0, 0, 0, time.UTC), "start", podKey{Pod: "Pod1"}),
				NewIntervalPoint(time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC), "end", podKey{Pod: "Pod1"}),
			},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			result := getIntervalPointsFromWindows(testCase.pvcIntervalMap)

			if len(result) != len(testCase.expected) {
				t.Errorf("getIntervalPointsFromWindows test failed: %s: Got %+v but expected %+v", testCase.name, result, testCase.expected)
			}

			for i := range testCase.expected {

				// For correctness in terms of individual position of IntervalPoints, we only need to check the time/type.
				// Key is used in other associated calculations, so it must exist, but order does not matter if other sorting
				// logic is obeyed.
				if !testCase.expected[i].Time.Equal(result[i].Time) || testCase.expected[i].PointType != result[i].PointType {
					t.Errorf("getIntervalPointsFromWindows test failed: %s: Got point %s:%s but expected %s:%s", testCase.name, testCase.expected[i].PointType, testCase.expected[i].Time, result[i].PointType, result[i].Time)
				}

			}

		})
	}
}

func TestGetPVCCostCoefficients(t *testing.T) {
	cases := []struct {
		name           string
		pvcIntervalMap map[podKey]kubecost.Window
		intervals      []IntervalPoint
		expected       map[podKey][][]float64
	}{
		{
			name: "four pods w/ various overlaps",
			pvcIntervalMap: map[podKey]kubecost.Window{
				// Pod running from 8 am to 9 am
				podKey{
					Pod: "Pod1",
				}: kubecost.Window(kubecost.NewClosedWindow(
					time.Date(2021, 2, 19, 8, 0, 0, 0, time.UTC),
					time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC),
				)),
				// Pod running from 8:30 am to 9 am
				podKey{
					Pod: "Pod2",
				}: kubecost.Window(kubecost.NewClosedWindow(
					time.Date(2021, 2, 19, 8, 30, 0, 0, time.UTC),
					time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC),
				)),
				// Pod running from 8:45 am to 9 am
				podKey{
					Pod: "Pod3",
				}: kubecost.Window(kubecost.NewClosedWindow(
					time.Date(2021, 2, 19, 8, 45, 0, 0, time.UTC),
					time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC),
				)),
				// Pod running from 8 am to 8:15 am
				podKey{
					Pod: "Pod4",
				}: kubecost.Window(kubecost.NewClosedWindow(
					time.Date(2021, 2, 19, 8, 0, 0, 0, time.UTC),
					time.Date(2021, 2, 19, 8, 15, 0, 0, time.UTC),
				)),
			},
			intervals: []IntervalPoint{
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 0, 0, 0, time.UTC), "start", podKey{Pod: "Pod1"}),
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 0, 0, 0, time.UTC), "start", podKey{Pod: "Pod4"}),
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 15, 0, 0, time.UTC), "end", podKey{Pod: "Pod4"}),
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 30, 0, 0, time.UTC), "start", podKey{Pod: "Pod2"}),
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 45, 0, 0, time.UTC), "start", podKey{Pod: "Pod3"}),
				NewIntervalPoint(time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC), "end", podKey{Pod: "Pod2"}),
				NewIntervalPoint(time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC), "end", podKey{Pod: "Pod3"}),
				NewIntervalPoint(time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC), "end", podKey{Pod: "Pod1"}),
			},
			expected: map[podKey][][]float64{
				podKey{
					Pod: "Pod1",
				}: [][]float64{
					[]float64{0.5, 0.25},
					[]float64{1, 0.25},
					[]float64{0.5, 0.25},
					[]float64{1.0 / 3.0, 0.25},
				},
				podKey{
					Pod: "Pod2",
				}: [][]float64{
					[]float64{0.5, 0.50},
					[]float64{1.0 / 3.0, 0.50},
				},
				podKey{
					Pod: "Pod3",
				}: [][]float64{
					[]float64{1.0 / 3.0, 1.0},
				},
				podKey{
					Pod: "Pod4",
				}: [][]float64{
					[]float64{0.5, 1.0},
				},
			},
		},
		{
			name: "two pods no overlap",
			pvcIntervalMap: map[podKey]kubecost.Window{
				// Pod running from 8 am to 8:30 am
				podKey{
					Pod: "Pod1",
				}: kubecost.Window(kubecost.NewClosedWindow(
					time.Date(2021, 2, 19, 8, 0, 0, 0, time.UTC),
					time.Date(2021, 2, 19, 8, 30, 0, 0, time.UTC),
				)),
				// Pod running from 8:30 am to 9 am
				podKey{
					Pod: "Pod2",
				}: kubecost.Window(kubecost.NewClosedWindow(
					time.Date(2021, 2, 19, 8, 30, 0, 0, time.UTC),
					time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC),
				)),
			},
			intervals: []IntervalPoint{
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 0, 0, 0, time.UTC), "start", podKey{Pod: "Pod1"}),
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 30, 0, 0, time.UTC), "start", podKey{Pod: "Pod2"}),
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 30, 0, 0, time.UTC), "end", podKey{Pod: "Pod1"}),
				NewIntervalPoint(time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC), "end", podKey{Pod: "Pod2"}),
			},
			expected: map[podKey][][]float64{
				podKey{
					Pod: "Pod1",
				}: [][]float64{
					[]float64{1.0, 1.0},
				},
				podKey{
					Pod: "Pod2",
				}: [][]float64{
					[]float64{1.0, 1.0},
				},
			},
		},
		{
			name: "two pods total overlap",
			pvcIntervalMap: map[podKey]kubecost.Window{
				// Pod running from 8:30 am to 9 am
				podKey{
					Pod: "Pod1",
				}: kubecost.Window(kubecost.NewClosedWindow(
					time.Date(2021, 2, 19, 8, 30, 0, 0, time.UTC),
					time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC),
				)),
				// Pod running from 8:30 am to 9 am
				podKey{
					Pod: "Pod2",
				}: kubecost.Window(kubecost.NewClosedWindow(
					time.Date(2021, 2, 19, 8, 30, 0, 0, time.UTC),
					time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC),
				)),
			},
			intervals: []IntervalPoint{
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 30, 0, 0, time.UTC), "start", podKey{Pod: "Pod1"}),
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 30, 0, 0, time.UTC), "start", podKey{Pod: "Pod2"}),
				NewIntervalPoint(time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC), "end", podKey{Pod: "Pod1"}),
				NewIntervalPoint(time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC), "end", podKey{Pod: "Pod2"}),
			},
			expected: map[podKey][][]float64{
				podKey{
					Pod: "Pod1",
				}: [][]float64{
					[]float64{0.5, 1.0},
				},
				podKey{
					Pod: "Pod2",
				}: [][]float64{
					[]float64{0.5, 1.0},
				},
			},
		},
		{
			name: "one pod",
			pvcIntervalMap: map[podKey]kubecost.Window{
				// Pod running from 8 am to 9 am
				podKey{
					Pod: "Pod1",
				}: kubecost.Window(kubecost.NewClosedWindow(
					time.Date(2021, 2, 19, 8, 0, 0, 0, time.UTC),
					time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC),
				)),
			},
			intervals: []IntervalPoint{
				NewIntervalPoint(time.Date(2021, 2, 19, 8, 0, 0, 0, time.UTC), "start", podKey{Pod: "Pod1"}),
				NewIntervalPoint(time.Date(2021, 2, 19, 9, 0, 0, 0, time.UTC), "end", podKey{Pod: "Pod1"}),
			},
			expected: map[podKey][][]float64{
				podKey{
					Pod: "Pod1",
				}: [][]float64{
					[]float64{1.0, 1.0},
				},
			},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			result := make(map[podKey][][]float64)
			getPVCCostCoefficients(testCase.intervals, testCase.pvcIntervalMap, result)

			if !reflect.DeepEqual(result, testCase.expected) {
				t.Errorf("getPVCCostCoefficients test failed: %s: Got %+v but expected %+v", testCase.name, result, testCase.expected)
			}
		})
	}
}
