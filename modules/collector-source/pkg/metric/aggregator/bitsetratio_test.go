package aggregator

import (
	"math"
	"reflect"
	"testing"
	"time"
)

func TestBitSetRatioAggregator_Value(t *testing.T) {
	time1 := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	time2 := time.Date(1, 1, 1, 0, 1, 0, 0, time.UTC)
	time3 := time.Date(1, 1, 1, 0, 2, 0, 0, time.UTC)
	time4 := time.Date(1, 1, 1, 0, 3, 0, 0, time.UTC)

	type update struct {
		value     float64
		timestamp time.Time
	}
	tests := map[string]struct {
		bit     uint64
		updates []update
		want    []MetricValue
	}{
		"no update": {
			bit:     0x8,
			updates: []update{},
			want:    []MetricValue{{Value: 0}},
		},
		"single sample bit set": {
			bit:     0x8,
			updates: []update{{value: 8, timestamp: time1}},
			want:    []MetricValue{{Value: 1}},
		},
		"single sample bit clear": {
			bit:     0x8,
			updates: []update{{value: 4, timestamp: time1}},
			want:    []MetricValue{{Value: 0}},
		},
		"half of samples set": {
			bit: 0x4,
			updates: []update{
				{value: 0x4, timestamp: time1},
				{value: 0x0, timestamp: time2},
				{value: 0x4 | 0x8, timestamp: time3},
				{value: 0x8, timestamp: time4},
			},
			want: []MetricValue{{Value: 0.5}},
		},
		"other bits do not count": {
			bit: 0x40,
			updates: []update{
				{value: 0x1 | 0x2 | 0x4 | 0x8 | 0x10 | 0x20 | 0x80, timestamp: time1},
			},
			want: []MetricValue{{Value: 0}},
		},
		"duplicate timestamp counts once": {
			bit: 0x4,
			updates: []update{
				{value: 0x4, timestamp: time1},
				{value: 0x0, timestamp: time1},
				{value: 0x0, timestamp: time2},
			},
			want: []MetricValue{{Value: 0.5}},
		},
		"invalid values are treated as bit clear": {
			bit: 0x4,
			updates: []update{
				{value: math.NaN(), timestamp: time1},
				{value: -4, timestamp: time2},
				{value: 0x4, timestamp: time3},
			},
			want: []MetricValue{{Value: 1.0 / 3.0}},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			agg := BitSetRatio(tt.bit)([]string{})
			for _, u := range tt.updates {
				agg.Update(u.value, u.timestamp, nil)
			}
			if got := agg.Value(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Value() = %v, want %v", got, tt.want)
			}
		})
	}
}
