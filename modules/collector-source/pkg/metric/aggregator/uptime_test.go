package aggregator

import (
	"reflect"
	"testing"
	"time"
)

func TestActiveMinutesAggregator_Value(t *testing.T) {
	time1 := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	time2 := time.Date(1, 1, 1, 0, 0, 1, 0, time.UTC)
	time3 := time.Date(1, 1, 1, 0, 0, 2, 0, time.UTC)
	type update struct {
		value                 float64
		timestamp             time.Time
		additionalInformation map[string]string
	}
	tests := map[string]struct {
		updates []update
		want    []MetricValue
	}{
		"no update": {
			updates: []update{},
			want:    []MetricValue{},
		},
		"single update": {
			updates: []update{
				{
					value:     1,
					timestamp: time1,
				},
			},
			want: []MetricValue{
				{
					Value:     1,
					Timestamp: &time1,
				},
			},
		},
		"two sequential updates": {
			updates: []update{
				{
					value:     2,
					timestamp: time1,
				},
				{
					value:     1,
					timestamp: time2,
				},
			},
			want: []MetricValue{
				{
					Value:     1,
					Timestamp: &time1,
				},
				{
					Value:     1,
					Timestamp: &time2,
				},
			},
		},
		"multi update on single time": {
			updates: []update{
				{
					value:     1,
					timestamp: time1,
				},
				{
					value:     2,
					timestamp: time1,
				},
			},
			want: []MetricValue{
				{
					Value:     1,
					Timestamp: &time1,
				},
			},
		},
		"three sequential updates": {
			updates: []update{
				{
					value:     1,
					timestamp: time1,
				},
				{
					value:     1,
					timestamp: time2,
				},
				{
					value:     1,
					timestamp: time3,
				},
			},
			want: []MetricValue{
				{
					Value:     1,
					Timestamp: &time1,
				},
				{
					Value:     1,
					Timestamp: &time3,
				},
			},
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			agg := uptimeAggregator{}
			for _, u := range tt.updates {
				agg.Update(u.value, u.timestamp, u.additionalInformation)
			}
			got := agg.Value()
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got = %v, want %v", got, tt.want)
			}
		})
	}
}
