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
	time3 := time.Date(1, 1, 1, 0, 2, 0, 0, time.UTC)

	above := QuantileOverTime(1.5)(nil)
	above.Update(2, time1, nil)
	above.Update(8, time2, nil)
	if got := above.Value()[0].Value; got != 8 {
		t.Errorf("phi above 1 should clamp to the maximum: got %v, want 8", got)
	}

	// With three or more samples, phi > 1 produces a rank whose floor
	// exceeds the last index; regression test for the index-out-of-range
	// panic that occurred before phi was clamped.
	aboveThree := QuantileOverTime(1.5)(nil)
	aboveThree.Update(2, time1, nil)
	aboveThree.Update(8, time2, nil)
	aboveThree.Update(5, time3, nil)
	if got := aboveThree.Value()[0].Value; got != 8 {
		t.Errorf("phi above 1 with 3 samples should clamp to the maximum: got %v, want 8", got)
	}

	below := QuantileOverTime(-0.5)(nil)
	below.Update(2, time1, nil)
	below.Update(8, time2, nil)
	if got := below.Value()[0].Value; got != 2 {
		t.Errorf("phi below 0 should clamp to the minimum: got %v, want 2", got)
	}

	// Mirror of the 3-sample case above: with 3+ samples, phi < 0 produced
	// a negative ceil(rank) index before phi was clamped.
	belowThree := QuantileOverTime(-0.5)(nil)
	belowThree.Update(2, time1, nil)
	belowThree.Update(8, time2, nil)
	belowThree.Update(5, time3, nil)
	if got := belowThree.Value()[0].Value; got != 2 {
		t.Errorf("phi below 0 with 3 samples should clamp to the minimum: got %v, want 2", got)
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

// TestQuantileOverTimeAggregator_PoolsAcrossSeries pins the documented
// precondition: Update carries no series identity, so samples from several
// series landing in one aggregator are pooled flat rather than combined per
// timestamp. The numbers are the worked example from the type comment.
//
// This is not asserting that flat pooling is desirable for a multi-series
// group; it asserts that the behaviour is what the doc says, so that a future
// change to the aggregator or to the collectors' grouping is caught here.
func TestQuantileOverTimeAggregator_PoolsAcrossSeries(t *testing.T) {
	time1 := time.Now()
	time2 := time1.Add(30 * time.Minute)
	time3 := time1.Add(time.Hour)

	agg := QuantileOverTime(0.95)(nil)
	// pod1 and pod2 reporting at the same three timestamps.
	agg.Update(2, time1, nil)
	agg.Update(3, time1, nil)
	agg.Update(4, time2, nil)
	agg.Update(5, time2, nil)
	agg.Update(1, time3, nil)
	agg.Update(1, time3, nil)

	// Flat pool [1,1,2,3,4,5]: rank = 0.95*5 = 4.75, interpolating 4 -> 5.
	const wantFlatPool = 4.75
	if got := agg.Value()[0].Value; got != wantFlatPool {
		t.Errorf("multi-series pool p95 = %v, want %v (flat pool, per the documented precondition)", got, wantFlatPool)
	}

	// The single-series case the inference collectors actually register is
	// unaffected: one sample per timestamp, quantile over that series alone.
	single := QuantileOverTime(0.95)(nil)
	single.Update(2, time1, nil)
	single.Update(4, time2, nil)
	single.Update(1, time3, nil)

	// Sorted [1,2,4]: rank = 0.95*2 = 1.9, interpolating 2 -> 4.
	const wantSingleSeries = 3.8
	if got := single.Value()[0].Value; got != wantSingleSeries {
		t.Errorf("single-series p95 = %v, want %v", got, wantSingleSeries)
	}
}
