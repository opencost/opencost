package aggregator

import (
	"reflect"
	"testing"
	"time"
)

func TestInfoAggregator_AdditionInfo(t *testing.T) {
	type update struct {
		value                 float64
		timestamp             time.Time
		additionalInformation map[string]string
	}
	tests := map[string]struct {
		updates []update
		want    map[string]string
	}{
		"no update": {
			updates: []update{},
			want:    nil,
		},
		"empty update": {
			updates: []update{
				{},
			},
			want: nil,
		},
		"single update": {
			updates: []update{
				{
					additionalInformation: map[string]string{
						"test": "test",
					},
				},
			},
			want: map[string]string{
				"test": "test",
			},
		},
		"double update": {
			updates: []update{
				{
					additionalInformation: map[string]string{
						"test": "test",
					},
				},
				{
					additionalInformation: map[string]string{
						"test2": "test2",
					},
				},
			},
			want: map[string]string{
				"test2": "test2",
			},
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			agg := infoAggregator{}
			for _, u := range tt.updates {
				agg.Update(u.value, u.timestamp, u.additionalInformation)
			}
			got := agg.AdditionInfo()
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got = %v, want %v", got, tt.want)
			}
		})
	}
}
