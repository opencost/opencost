package aggregator

import (
	"reflect"
	"testing"
	"time"
)

func TestMaxOverTimeAggregator_Value(t *testing.T) {
	time1 := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
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
			want: []MetricValue{
				{
					Value: 0,
				},
			},
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
					Value: 1,
				},
			},
		},
		"max first": {
			updates: []update{
				{
					value:     2,
					timestamp: time1,
				},
				{
					value:     1,
					timestamp: time1,
				},
			},
			want: []MetricValue{
				{
					Value: 2,
				},
			},
		},
		"max last": {
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
					Value: 2,
				},
			},
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			agg := maxOverTimeAggregator{}
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
