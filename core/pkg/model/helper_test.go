package model

import (
	"reflect"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/model/pb"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestConvertWindow(t *testing.T) {
	timeDay := time.Date(2025, 9, 1, 0, 0, 0, 0, time.UTC)
	timeHour := timeDay.Add(time.Hour)
	timeTenMinute := timeHour.Add(time.Minute * 10)
	invalidTime := timeTenMinute.Add(time.Second)
	tests := []struct {
		name    string
		window  *pb.Window
		want    opencost.Window
		wantErr bool
	}{
		{
			name:    "nil window",
			window:  nil,
			want:    opencost.Window{},
			wantErr: true,
		},
		{
			name: "invalid resolution",
			window: &pb.Window{
				Resolution: "invalid",
				Start:      timestamppb.New(timeDay),
			},
			want:    opencost.Window{},
			wantErr: true,
		},
		{
			name: "invalid time",
			window: &pb.Window{
				Resolution: "1d",
				Start:      timestamppb.New(invalidTime),
			},
			want:    opencost.Window{},
			wantErr: true,
		},
		{
			name: "valid 1d",
			window: &pb.Window{
				Resolution: "1d",
				Start:      timestamppb.New(timeDay),
			},
			want:    opencost.NewClosedWindow(timeDay, timeDay.Add(timeutil.Day)),
			wantErr: false,
		},
		{
			name: "valid 1h",
			window: &pb.Window{
				Resolution: "1h",
				Start:      timestamppb.New(timeHour),
			},
			want:    opencost.NewClosedWindow(timeHour, timeHour.Add(time.Hour)),
			wantErr: false,
		},
		{
			name: "valid 10m",
			window: &pb.Window{
				Resolution: "10m",
				Start:      timestamppb.New(timeTenMinute),
			},
			want:    opencost.NewClosedWindow(timeTenMinute, timeTenMinute.Add(10*time.Minute)),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ConvertWindow(tt.window)
			if (err != nil) != tt.wantErr {
				t.Errorf("ConvertWindow() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ConvertWindow() got = %v, want %v", got, tt.want)
			}
		})
	}
}
