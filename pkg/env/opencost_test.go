package env

import (
	"os"
	"testing"
)

func TestGetAPIPort(t *testing.T) {
	tests := []struct {
		name string
		want int
		pre  func()
	}{
		{
			name: "Ensure the default API port '9003'",
			want: 9003,
		},
		{
			name: "Ensure the default API port '9003' when API_PORT is set to ''",
			want: 9003,
			pre: func() {
				os.Setenv("API_PORT", "")
			},
		},
		{
			name: "Ensure the API port '9004' when API_PORT is set to '9004'",
			want: 9004,
			pre: func() {
				os.Setenv("API_PORT", "9004")
			},
		},
	}
	for _, tt := range tests {
		if tt.pre != nil {
			tt.pre()
		}
		t.Run(tt.name, func(t *testing.T) {
			if got := GetOpencostAPIPort(); got != tt.want {
				t.Errorf("GetAPIPort() = %v, want %v", got, tt.want)
			}
		})
	}

}
