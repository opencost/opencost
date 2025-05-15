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
			if got := GetAPIPort(); got != tt.want {
				t.Errorf("GetAPIPort() = %v, want %v", got, tt.want)
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

func TestGetKubernetesEnabled(t *testing.T) {
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
			name: "Ensure the value is true when KUBERNETES_PORT has a value",
			want: true,
			pre: func() {
				os.Setenv("KUBERNETES_PORT", "tcp://10.43.0.1:443")
			},
		},
	}
	for _, tt := range tests {
		if tt.pre != nil {
			tt.pre()
		}
		t.Run(tt.name, func(t *testing.T) {
			if got := IsKubernetesEnabled(); got != tt.want {
				t.Errorf("IsKubernetesEnabled() = %v, want %v", got, tt.want)
			}
		})
	}

}

func TestGetCloudCostConfigPath(t *testing.T) {
	tests := []struct {
		name string
		want string
		pre  func()
	}{
		{
			name: "Ensure the default value is 'cloud-integration.json'",
			want: "cloud-integration.json",
		},
		{
			name: "Ensure the value is 'cloud-integration.json' when CLOUD_COST_CONFIG_PATH is set to ''",
			want: "cloud-integration.json",
			pre: func() {
				os.Setenv("CLOUD_COST_CONFIG_PATH", "")
			},
		},
		{
			name: "Ensure the value is 'flying-pig.json' when CLOUD_COST_CONFIG_PATH is set to 'flying-pig.json'",
			want: "flying-pig.json",
			pre: func() {
				os.Setenv("CLOUD_COST_CONFIG_PATH", "flying-pig.json")
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

func TestEnvVarsWithBackup(t *testing.T) {
	t.Run("test install namespace env var", func(t *testing.T) {
		t.Setenv(InstallNamespaceEnvVar, "test-namespace")
		t.Setenv(KubecostNamespaceEnvVar, "kubecost-test-namespace")

		ns := GetInstallNamespace()
		if ns != "test-namespace" {
			t.Errorf("Expected install namespace to be 'test-namespace', got '%s'", ns)
		}
	})
	t.Run("test kubecost namespace env var", func(t *testing.T) {
		t.Setenv(KubecostNamespaceEnvVar, "kc-test-namespace")

		ns := GetInstallNamespace()

		if ns != "kc-test-namespace" {
			t.Errorf("Expected install namespace to be 'kc-test-namespace', got '%s'", ns)
		}
	})

	t.Run("test default install namespace", func(t *testing.T) {
		t.Setenv(InstallNamespaceEnvVar, "test-namespace")

		ns := GetInstallNamespace()

		if ns != "test-namespace" {
			t.Errorf("Expected default install namespace to be 'test-namespace', got '%s'", ns)
		}
	})

	t.Run("test default install namespace", func(t *testing.T) {
		ns := GetInstallNamespace()

		if ns != "opencost" {
			t.Errorf("Expected default install namespace to be 'opencost', got '%s'", ns)
		}
	})

	t.Run("test config bucket file with both", func(t *testing.T) {
		t.Setenv(ConfigBucketEnvVar, "test-bucket")
		t.Setenv(KubecostConfigBucketEnvVar, "kc-test-bucket")

		configBucketFile := GetConfigBucketFile()

		if configBucketFile != "test-bucket" {
			t.Errorf("Expected config bucket file to be 'test-bucket', got '%s'", configBucketFile)
		}
	})

	t.Run("test config bucket file with kc", func(t *testing.T) {
		t.Setenv(KubecostConfigBucketEnvVar, "kc-test-bucket")

		configBucketFile := GetConfigBucketFile()

		if configBucketFile != "kc-test-bucket" {
			t.Errorf("Expected config bucket file to be 'kc-test-bucket', got '%s'", configBucketFile)
		}
	})

	t.Run("test config bucket file with single", func(t *testing.T) {
		t.Setenv(ConfigBucketEnvVar, "test-bucket")

		configBucketFile := GetConfigBucketFile()

		if configBucketFile != "test-bucket" {
			t.Errorf("Expected config bucket file to be 'test-bucket', got '%s'", configBucketFile)
		}
	})

	t.Run("test config bucket file with both", func(t *testing.T) {
		configBucketFile := GetConfigBucketFile()

		if configBucketFile != "" {
			t.Errorf("Expected config bucket file to be '', got '%s'", configBucketFile)
		}
	})

}
