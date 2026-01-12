package env

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/env"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestGetMCPQueryTimeout_Default(t *testing.T) {
	// Ensure env var is not set
	os.Unsetenv(MCPQueryTimeoutSecondsEnvVar)

	timeout := GetMCPQueryTimeout()
	assert.Equal(t, 60*time.Second, timeout, "Default timeout should be 60 seconds")
}

func TestGetMCPQueryTimeout_CustomValue(t *testing.T) {
	// Set custom timeout
	err := os.Setenv(MCPQueryTimeoutSecondsEnvVar, "120")
	require.NoError(t, err)
	defer os.Unsetenv(MCPQueryTimeoutSecondsEnvVar)

	timeout := GetMCPQueryTimeout()
	assert.Equal(t, 120*time.Second, timeout, "Custom timeout should be 120 seconds")
}

func TestGetMCPQueryTimeout_InvalidValue(t *testing.T) {
	// Set invalid value (should fall back to default)
	err := os.Setenv(MCPQueryTimeoutSecondsEnvVar, "invalid")
	require.NoError(t, err)
	defer os.Unsetenv(MCPQueryTimeoutSecondsEnvVar)

	timeout := GetMCPQueryTimeout()
	assert.Equal(t, 60*time.Second, timeout, "Invalid value should fall back to default 60 seconds")
}

func TestGetMCPQueryTimeout_ZeroValue(t *testing.T) {
	// Set zero value - should fall back to minimum of 1 second
	err := os.Setenv(MCPQueryTimeoutSecondsEnvVar, "0")
	require.NoError(t, err)
	defer os.Unsetenv(MCPQueryTimeoutSecondsEnvVar)

	timeout := GetMCPQueryTimeout()
	assert.Equal(t, 1*time.Second, timeout, "Zero value should use minimum of 1 second")
}

func TestGetMCPQueryTimeout_NegativeValue(t *testing.T) {
	// Set negative value - should fall back to minimum of 1 second
	err := os.Setenv(MCPQueryTimeoutSecondsEnvVar, "-10")
	require.NoError(t, err)
	defer os.Unsetenv(MCPQueryTimeoutSecondsEnvVar)

	timeout := GetMCPQueryTimeout()
	assert.Equal(t, 1*time.Second, timeout, "Negative value should use minimum of 1 second")
}

func TestGetMCPQueryTimeout_LargeValue(t *testing.T) {
	// Set large timeout value
	err := os.Setenv(MCPQueryTimeoutSecondsEnvVar, "3600")
	require.NoError(t, err)
	defer os.Unsetenv(MCPQueryTimeoutSecondsEnvVar)

	timeout := GetMCPQueryTimeout()
	assert.Equal(t, 3600*time.Second, timeout, "Large timeout should be accepted (1 hour)")
}
