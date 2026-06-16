package aggregator

import (
	"reflect"
	"testing"
	"time"
)

func TestChangesAggregator_Value(t *testing.T) {
	time1 := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	time2 := time.Date(1, 1, 1, 0, 1, 0, 0, time.UTC)
	time3 := time.Date(1, 1, 1, 0, 2, 0, 0, time.UTC)
	time4 := time.Date(1, 1, 1, 0, 3, 0, 0, time.UTC)

	type update struct {
		value     float64
		timestamp time.Time
	}
	tests := map[string]struct {
		updates []update
		want    []MetricValue
	}{
		"no update": {
			updates: []update{},
			want:    []MetricValue{{Value: 0}},
		},
		"single sample is zero changes": {
			updates: []update{{value: 13, timestamp: time1}},
			want:    []MetricValue{{Value: 0}},
		},
		"constant value is zero changes": {
			updates: []update{
				{value: 0, timestamp: time1},
				{value: 0, timestamp: time2},
				{value: 0, timestamp: time3},
			},
			want: []MetricValue{{Value: 0}},
		},
		"each transition counts": {
			updates: []update{
				{value: 0, timestamp: time1},
				{value: 13, timestamp: time2},
				{value: 13, timestamp: time3},
				{value: 31, timestamp: time4},
			},
			want: []MetricValue{{Value: 2}},
		},
		"out of order updates are ignored": {
			updates: []update{
				{value: 0, timestamp: time2},
				{value: 13, timestamp: time1},
			},
			want: []MetricValue{{Value: 0}},
		},
		"duplicate timestamp is ignored": {
			updates: []update{
				{value: 0, timestamp: time1},
				{value: 13, timestamp: time1},
			},
			want: []MetricValue{{Value: 0}},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			agg := Changes([]string{})
			for _, u := range tt.updates {
				agg.Update(u.value, u.timestamp, nil)
			}
			if got := agg.Value(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Value() = %v, want %v", got, tt.want)
			}
		})
	}
}
