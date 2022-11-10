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
			name: "Ensure the value is false when DISABLE_CACHE is set to valse",
			want: false,
			pre: func() {
				os.Setenv("DISABLE_CACHE", "false")
			},
		},
		{
			name: "Ensure the value is false when DISABLE_CACHE is set to valse",
			want: true,
			pre: func() {
				os.Setenv("DISABLE_CACHE", "true")
			},
		},
	}
	for _, tt := range tests {
		if tt.pre != nil {
			tt.pre()
		}
		t.Run(tt.name, func(t *testing.T) {
			if got := IsCacheDisabled(); got != tt.want {
				t.Errorf("IsCacheDisabled() = %v, want %v", got, tt.want)
			}
		})
	}
}
