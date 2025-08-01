package env

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/env"
)

func TestGetCloudCostConfigPath(t *testing.T) {
	tests := []struct {
		name string
		want string
		pre  func()
	}{
		{
			name: "Ensure the default value is 'cloud-integration.json'",
			want: "/var/configs/cloud-integration.json",
		},
		{
			name: "Ensure the value is 'cloud-integration.json' when CLOUD_COST_CONFIG_PATH is set to ''",
			want: "/test/cloud-integration.json",
			pre: func() {
				env.Set(env.ConfigPathEnvVar, "/test")
			},
		},
	}
	for _, tt := range tests {
		if tt.pre != nil {
			tt.pre()
		}
		t.Run(tt.name, func(t *testing.T) {
			if got := GetCloudCostConfigPath(); got != tt.want {
				t.Errorf("GetCloudCostConfigPath() = %v, want %v", got, tt.want)
			}
		})
	}

}
