package env

import (
	"os"
	"testing"
)

func TestIsCacheDisabled(t *testing.T) {
	tests := []struct {
		name string
		want bool
		pre  func()
	}{
		{
			name: "Ensure the default value is false",
			want: false,
		},
		{
			name: "Ensure the value is false when DISABLE_AGGREGATE_COST_MODEL_CACHE is set to false",
			want: false,
			pre: func() {
				os.Setenv("DISABLE_AGGREGATE_COST_MODEL_CACHE", "false")
			},
		},
		{
			name: "Ensure the value is true when DISABLE_AGGREGATE_COST_MODEL_CACHE is set to true",
			want: true,
			pre: func() {
				os.Setenv("DISABLE_AGGREGATE_COST_MODEL_CACHE", "true")
			},
		},
	}
	for _, tt := range tests {
		if tt.pre != nil {
			tt.pre()
		}
		t.Run(tt.name, func(t *testing.T) {
			if got := IsAggregateCostModelCacheDisabled(); got != tt.want {
				t.Errorf("IsAggregateCostModelCacheDisabled() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetExportCSVMaxDays(t *testing.T) {
	tests := []struct {
		name string
		want int
		pre  func()
	}{
		{
			name: "Ensure the default value is 90d",
			want: 90,
		},
		{
			name: "Ensure the value is 30 when EXPORT_CSV_MAX_DAYS is set to 30",
			want: 30,
			pre: func() {
				os.Setenv("EXPORT_CSV_MAX_DAYS", "30")
			},
		},
		{
			name: "Ensure the value is 90 when EXPORT_CSV_MAX_DAYS is set to empty string",
			want: 90,
			pre: func() {
				os.Setenv("EXPORT_CSV_MAX_DAYS", "")
			},
		},
		{
			name: "Ensure the value is 90 when EXPORT_CSV_MAX_DAYS is set to invalid value",
			want: 90,
			pre: func() {
				os.Setenv("EXPORT_CSV_MAX_DAYS", "foo")
			},
		},
	}
	for _, tt := range tests {
		if tt.pre != nil {
			tt.pre()
		}
		t.Run(tt.name, func(t *testing.T) {
			if got := GetExportCSVMaxDays(); got != tt.want {
				t.Errorf("GetExportCSVMaxDays() = %v, want %v", got, tt.want)
			}
		})
	}
}
