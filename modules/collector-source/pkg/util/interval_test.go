package util

import (
	"reflect"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

func TestNewInterval(t *testing.T) {
	tests := map[string]struct {
		def     string
		want    Interval
		wantErr bool
	}{
		"invalid": {
			def:     "invalid",
			want:    nil,
			wantErr: true,
		},
		"invalid2": {
			def:     "1M",
			want:    nil,
			wantErr: true,
		},
		"invalid3": {
			def:     "d20",
			want:    nil,
			wantErr: true,
		},
		"one minute": {
			def: "1m",
			want: &durationInterval{
				duration: time.Minute,
			},
			wantErr: false,
		},
		"ten minute": {
			def: "10m",
			want: &durationInterval{
				duration: time.Minute * 10,
			},
			wantErr: false,
		},
		"one hour": {
			def: "1h",
			want: &durationInterval{
				duration: time.Hour,
			},
		},
		"six hours": {
			def: "6h",
			want: &durationInterval{
				duration: time.Hour * 6,
			},
		},
		"one day": {
			def: "1d",
			want: &durationInterval{
				duration: timeutil.Day,
			},
		},
		"seven days": {
			def: "7d",
			want: &durationInterval{
				duration: timeutil.Day * 7,
			},
		},
		"one week": {
			def: "1w",
			want: &weekInterval{
				count: 1,
			},
		},
		"two weeks": {
			def: "2w",
			want: &weekInterval{
				count: 2,
			},
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := NewInterval(tt.def)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewInterval() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewInterval() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_durationInterval_Add(t *testing.T) {

	type args struct {
		t time.Time
		i int
	}
	tests := map[string]struct {
		duration time.Duration
		args     args
		want     time.Time
	}{
		"day interval add 1": {
			duration: timeutil.Day,
			args: args{
				t: time.Date(2025, time.April, 2, 0, 0, 0, 0, time.UTC),
				i: 1,
			},
			want: time.Date(2025, time.April, 3, 0, 0, 0, 0, time.UTC),
		},
		"day interval sub 1": {
			duration: timeutil.Day,
			args: args{
				t: time.Date(2025, time.April, 2, 0, 0, 0, 0, time.UTC),
				i: -1,
			},
			want: time.Date(2025, time.April, 1, 0, 0, 0, 0, time.UTC),
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			d := &durationInterval{
				duration: tt.duration,
			}
			if got := d.Add(tt.args.t, tt.args.i); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Add() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_durationInterval_Truncate(t *testing.T) {
	tests := map[string]struct {
		duration time.Duration
		input    time.Time
		want     time.Time
	}{
		"one day truncate": {
			duration: timeutil.Day,
			input:    time.Date(2025, time.April, 7, 3, 0, 0, 0, time.UTC),
			want:     time.Date(2025, time.April, 7, 0, 0, 0, 0, time.UTC),
		},
		"two day truncate": {
			duration: 2 * timeutil.Day,
			input:    time.Date(2025, time.April, 7, 3, 0, 0, 0, time.UTC),
			want:     time.Date(2025, time.April, 6, 0, 0, 0, 0, time.UTC),
		},
		"two day truncate 2": {
			duration: 2 * timeutil.Day,
			input:    time.Date(2025, time.April, 6, 3, 0, 0, 0, time.UTC),
			want:     time.Date(2025, time.April, 6, 0, 0, 0, 0, time.UTC),
		},
		"seven day truncate": {
			duration: 7 * timeutil.Day,
			input:    time.Date(2025, time.April, 7, 3, 0, 0, 0, time.UTC),
			want:     time.Date(2025, time.April, 7, 0, 0, 0, 0, time.UTC),
		},
		"seven day truncate 2": {
			duration: 7 * timeutil.Day,
			input:    time.Date(2025, time.March, 7, 3, 0, 0, 0, time.UTC),
			want:     time.Date(2025, time.March, 3, 0, 0, 0, 0, time.UTC),
		},
		"seven day truncate 3": {
			duration: 7 * timeutil.Day,
			input:    time.Date(2025, time.March, 3, 0, 0, 0, 0, time.UTC),
			want:     time.Date(2025, time.March, 3, 0, 0, 0, 0, time.UTC),
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			d := &durationInterval{
				duration: tt.duration,
			}
			if got := d.Truncate(tt.input); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Truncate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_weekInterval_Add(t *testing.T) {

	tests := map[string]struct {
		count int
		t     time.Time
		num   int
		want  time.Time
	}{
		"one week add one": {
			count: 1,
			t:     time.Date(2025, time.April, 2, 0, 0, 0, 0, time.UTC),
			num:   1,
			want:  time.Date(2025, time.April, 9, 0, 0, 0, 0, time.UTC),
		},
		"one week subtract one": {
			count: 1,
			t:     time.Date(2025, time.April, 9, 0, 0, 0, 0, time.UTC),
			num:   -1,
			want:  time.Date(2025, time.April, 2, 0, 0, 0, 0, time.UTC),
		},
		"two week add one": {
			count: 1,
			t:     time.Date(2025, time.April, 2, 0, 0, 0, 0, time.UTC),
			num:   2,
			want:  time.Date(2025, time.April, 16, 0, 0, 0, 0, time.UTC),
		},
		"one week add two": {
			count: 2,
			t:     time.Date(2025, time.April, 2, 0, 0, 0, 0, time.UTC),
			num:   1,
			want:  time.Date(2025, time.April, 16, 0, 0, 0, 0, time.UTC),
		},
		"two week add two": {
			count: 2,
			t:     time.Date(2025, time.April, 2, 0, 0, 0, 0, time.UTC),
			num:   2,
			want:  time.Date(2025, time.April, 30, 0, 0, 0, 0, time.UTC),
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			w := &weekInterval{
				count: tt.count,
			}
			if got := w.Add(tt.t, tt.num); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Add() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_weekInterval_Truncate(t *testing.T) {

	tests := map[string]struct {
		count int
		input time.Time
		want  time.Time
	}{
		"one week no change": {
			count: 1,
			input: time.Date(2025, time.April, 6, 0, 0, 0, 0, time.UTC),
			want:  time.Date(2025, time.April, 6, 0, 0, 0, 0, time.UTC),
		},

		"one week": {
			count: 1,
			input: time.Date(2025, time.April, 7, 3, 0, 0, 0, time.UTC),
			want:  time.Date(2025, time.April, 6, 0, 0, 0, 0, time.UTC),
		},
		"one week 2": {
			count: 1,
			input: time.Date(2025, time.March, 7, 3, 0, 0, 0, time.UTC),
			want:  time.Date(2025, time.March, 2, 0, 0, 0, 0, time.UTC),
		},
		"one week 3": {
			count: 1,
			input: time.Date(2025, time.March, 3, 0, 0, 0, 0, time.UTC),
			want:  time.Date(2025, time.March, 2, 0, 0, 0, 0, time.UTC),
		},
		"two week no change": {
			count: 2,
			input: time.Date(2025, time.March, 30, 0, 0, 0, 0, time.UTC),
			want:  time.Date(2025, time.March, 30, 0, 0, 0, 0, time.UTC),
		},
		"two week": {
			count: 2,
			input: time.Date(2025, time.April, 6, 0, 0, 0, 0, time.UTC),
			want:  time.Date(2025, time.March, 30, 0, 0, 0, 0, time.UTC),
		},
		"two week 2": {
			count: 2,
			input: time.Date(2025, time.April, 13, 0, 0, 0, 0, time.UTC),
			want:  time.Date(2025, time.April, 13, 0, 0, 0, 0, time.UTC),
		},
		"three week": {
			count: 3,
			input: time.Date(2025, time.April, 7, 0, 0, 0, 0, time.UTC),
			want:  time.Date(2025, time.April, 6, 0, 0, 0, 0, time.UTC),
		},
		"three week 2": {
			count: 3,
			input: time.Date(2025, time.April, 14, 0, 0, 0, 0, time.UTC),
			want:  time.Date(2025, time.April, 6, 0, 0, 0, 0, time.UTC),
		},
		"one week first week": {
			count: 1,
			input: time.Date(1, time.January, 6, 0, 0, 0, 0, time.UTC),
			want:  time.Time{}.Add(-1 * timeutil.Day),
		},
		"one week second week": {
			count: 1,
			input: time.Date(1, time.January, 9, 0, 0, 0, 0, time.UTC),
			want:  time.Date(1, time.January, 7, 0, 0, 0, 0, time.UTC),
		},
		"two week second week": {
			count: 2,
			input: time.Date(1, time.January, 9, 0, 0, 0, 0, time.UTC),
			want:  time.Time{}.Add(-1 * timeutil.Day),
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			w := &weekInterval{
				count: tt.count,
			}
			got := w.Truncate(tt.input)
			if got.Weekday() != time.Sunday {
				t.Errorf("result was not a sunday: %s", got.Weekday().String())
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Truncate() = %v, want %v", got, tt.want)
			}
		})
	}
}
