package env

import (
	"fmt"
	"os"
	"testing"

	"github.com/opencost/opencost/core/pkg/env"
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
			name: fmt.Sprintf("Ensure the default API port '9003' when %s is set to ''", env.APIPortEnvVar),
			want: 9003,
			pre: func() {
				os.Setenv(env.APIPortEnvVar, "")
			},
		},
		{
			name: fmt.Sprintf("Ensure the API port '9004' when %s is set to '9004'", env.APIPortEnvVar),
			want: 9004,
			pre: func() {
				os.Setenv(env.APIPortEnvVar, "9004")
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
