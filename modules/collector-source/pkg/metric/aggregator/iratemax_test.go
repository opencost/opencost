package aggregator

import (
	"reflect"
	"testing"
	"time"
)

func TestIRateMaxAggregator_Value(t *testing.T) {
	time1 := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	time2 := time.Date(1, 1, 1, 0, 0, 1, 0, time.UTC)
	time3 := time.Date(1, 1, 1, 0, 0, 2, 0, time.UTC)
	time4 := time.Date(1, 1, 1, 0, 0, 4, 0, time.UTC)
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
					Value: 0,
				},
			},
		},
		"normal increase": {
			updates: []update{
				{
					value:     1,
					timestamp: time1,
				},
				{
					value:     2,
					timestamp: time2,
				},
			},
			want: []MetricValue{
				{
					Value: 1,
				},
			},
		},
		"multi increase": {
			updates: []update{
				{
					value:     1,
					timestamp: time1,
				},
				{
					value:     2,
					timestamp: time2,
				},
				{
					value:     4,
					timestamp: time3,
				},
			},
			want: []MetricValue{
				{
					Value: 2,
				},
			},
		},
		"aggregated increase": {
			updates: []update{
				{
					value:     1,
					timestamp: time1,
				},
				{
					value:     2,
					timestamp: time1,
				},
				{
					value:     3,
					timestamp: time2,
				},
				{
					value:     4,
					timestamp: time2,
				},
			},
			want: []MetricValue{
				{
					Value: 4,
				},
			},
		},
		"missing sample": {
			updates: []update{
				{
					value:     1,
					timestamp: time1,
				},
				{
					value:     3,
					timestamp: time2,
				},
				{
					value:     6,
					timestamp: time4,
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
			agg := iRateMaxAggregator{}
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
