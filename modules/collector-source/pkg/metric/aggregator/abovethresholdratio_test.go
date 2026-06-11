package aggregator

import (
	"math"
	"reflect"
	"testing"
	"time"
)

func TestAboveThresholdRatioAggregator_Value(t *testing.T) {
	time1 := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	time2 := time.Date(1, 1, 1, 0, 1, 0, 0, time.UTC)
	time3 := time.Date(1, 1, 1, 0, 2, 0, 0, time.UTC)
	time4 := time.Date(1, 1, 1, 0, 3, 0, 0, time.UTC)

	type update struct {
		value     float64
		timestamp time.Time
	}
	tests := map[string]struct {
		threshold float64
		updates   []update
		want      []MetricValue
	}{
		"no update": {
			threshold: 0.9,
			updates:   []update{},
			want:      []MetricValue{{Value: 0}},
		},
		"all above": {
			threshold: 0.9,
			updates: []update{
				{value: 0.95, timestamp: time1},
				{value: 0.99, timestamp: time2},
			},
			want: []MetricValue{{Value: 1}},
		},
		"threshold is inclusive": {
			threshold: 0.9,
			updates:   []update{{value: 0.9, timestamp: time1}},
			want:      []MetricValue{{Value: 1}},
		},
		"quarter above": {
			threshold: 0.9,
			updates: []update{
				{value: 0.95, timestamp: time1},
				{value: 0.5, timestamp: time2},
				{value: 0.89, timestamp: time3},
				{value: 0.1, timestamp: time4},
			},
			want: []MetricValue{{Value: 0.25}},
		},
		"duplicate timestamp counts once": {
			threshold: 0.9,
			updates: []update{
				{value: 0.95, timestamp: time1},
				{value: 0.1, timestamp: time1},
				{value: 0.1, timestamp: time2},
			},
			want: []MetricValue{{Value: 0.5}},
		},
		"NaN counts as below": {
			threshold: 0.9,
			updates: []update{
				{value: math.NaN(), timestamp: time1},
				{value: 0.95, timestamp: time2},
			},
			want: []MetricValue{{Value: 0.5}},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			agg := AboveThresholdRatio(tt.threshold)([]string{})
			for _, u := range tt.updates {
				agg.Update(u.value, u.timestamp, nil)
			}
			if got := agg.Value(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Value() = %v, want %v", got, tt.want)
			}
		})
	}
}
