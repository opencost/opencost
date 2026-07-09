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

func TestQuantileOverTimeAggregator_PhiOutOfRangeClamps(t *testing.T) {
	time1 := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	time2 := time.Date(1, 1, 1, 0, 1, 0, 0, time.UTC)

	above := QuantileOverTime(1.5)(nil)
	above.Update(2, time1, nil)
	above.Update(8, time2, nil)
	if got := above.Value()[0].Value; got != 8 {
		t.Errorf("phi above 1 should clamp to the maximum: got %v, want 8", got)
	}

	below := QuantileOverTime(-0.5)(nil)
	below.Update(2, time1, nil)
	below.Update(8, time2, nil)
	if got := below.Value()[0].Value; got != 2 {
		t.Errorf("phi below 0 should clamp to the minimum: got %v, want 2", got)
	}
}

func TestQuantileOverTimeAggregator_Metadata(t *testing.T) {
	labelValues := []string{"model", "ns", "pod"}
	agg := QuantileOverTime(0.95)(labelValues)

	if got := agg.AdditionInfo(); got != nil {
		t.Errorf("AdditionInfo() = %v, want nil", got)
	}
	if got := agg.LabelValues(); !reflect.DeepEqual(got, labelValues) {
		t.Errorf("LabelValues() = %v, want %v", got, labelValues)
	}
}
