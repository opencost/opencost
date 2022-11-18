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
