package aggregator

import (
	"reflect"
	"testing"
	"time"
)

func TestQuantileOverTimeAggregator_Value(t *testing.T) {
	time1 := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	time2 := time.Date(1, 1, 1, 0, 1, 0, 0, time.UTC)
	time3 := time.Date(1, 1, 1, 0, 2, 0, 0, time.UTC)
	type update struct {
		value     float64
		timestamp time.Time
	}
	tests := map[string]struct {
		phi     float64
		updates []update
		want    []MetricValue
	}{
		"no update": {
			phi:     0.95,
			updates: []update{},
			want: []MetricValue{
				{Value: 0},
			},
		},
		"single update returns the value for any phi": {
			phi: 0.95,
			updates: []update{
				{value: 3, timestamp: time1},
			},
			want: []MetricValue{
				{Value: 3},
			},
		},
		"p95 interpolates between two samples": {
			phi: 0.95,
			updates: []update{
				{value: 0.2, timestamp: time1},
				{value: 0.8, timestamp: time2},
			},
			want: []MetricValue{
				{Value: 0.2 + (0.8-0.2)*0.95},
			},
		},
		"median of three samples": {
			phi: 0.5,
			updates: []update{
				{value: 10, timestamp: time1},
				{value: 1, timestamp: time2},
				{value: 5, timestamp: time3},
			},
			want: []MetricValue{
				{Value: 5},
			},
		},
		"p0 is the minimum and p1 is the maximum": {
			phi: 0,
			updates: []update{
				{value: 4, timestamp: time1},
				{value: 2, timestamp: time2},
				{value: 9, timestamp: time3},
			},
			want: []MetricValue{
				{Value: 2},
			},
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			agg := QuantileOverTime(tt.phi)(nil)
			for _, u := range tt.updates {
				agg.Update(u.value, u.timestamp, nil)
			}
			if got := agg.Value(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Value() = %v, want %v", got, tt.want)
			}
		})
	}
}
