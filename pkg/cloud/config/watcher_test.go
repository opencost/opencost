package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/opencost/opencost/core/pkg/env"
	pkgenv "github.com/opencost/opencost/pkg/env"
)

// TestMultiCloudWatcher_GetConfigs_PathFallback tests the path fallback logic
// for finding the cloud integration config file. This addresses issue #3450
// where the config file location changed between versions.
func TestMultiCloudWatcher_GetConfigs_PathFallback(t *testing.T) {
	// Sample valid config content
	validConfig := `{
		"aws": [{
			"athenaTable": "test_table",
			"athenaDatabase": "test_db",
			"athenaBucketName": "test-bucket",
			"athenaRegion": "us-east-1",
			"athenaWorkgroup": "primary",
			"serviceAccountName": "test-key",
			"serviceAccountSecret": "test-secret",
			"accountID": "123456789012"
		}]
	}`

	tests := []struct {
		name          string
		setupFiles    func(t *testing.T) (cleanup func())
		expectedFound bool
		expectedPath  string
		configPathEnv string
	}{
		{
			name: "config exists at primary path",
			setupFiles: func(t *testing.T) func() {
				tmpDir := t.TempDir()
				configPath := filepath.Join(tmpDir, "cloud-integration.json")
				if err := os.WriteFile(configPath, []byte(validConfig), 0644); err != nil {
					t.Fatalf("failed to write test config: %v", err)
				}
				env.Set(env.ConfigPathEnvVar, tmpDir)
				return func() {
					env.Set(env.ConfigPathEnvVar, "/var/configs")
				}
			},
			expectedFound: true,
			expectedPath:  "cloud-integration.json", // relative to tmpDir
		},
		{
			name: "config exists at subdirectory path (fixes #3450)",
			setupFiles: func(t *testing.T) func() {
				tmpDir := t.TempDir()
				// Create subdirectory
				subDir := filepath.Join(tmpDir, "cloud-integration")
				if err := os.MkdirAll(subDir, 0755); err != nil {
					t.Fatalf("failed to create subdirectory: %v", err)
				}
				configPath := filepath.Join(subDir, "cloud-integration.json")
				if err := os.WriteFile(configPath, []byte(validConfig), 0644); err != nil {
					t.Fatalf("failed to write test config: %v", err)
				}
				env.Set(env.ConfigPathEnvVar, tmpDir)
				return func() {
					env.Set(env.ConfigPathEnvVar, "/var/configs")
				}
			},
			expectedFound: true,
			expectedPath:  "cloud-integration/cloud-integration.json", // relative to tmpDir
		},
		{
			name: "config exists at legacy path",
			setupFiles: func(t *testing.T) func() {
				tmpDir := t.TempDir()
				// Create /var subdirectory structure
				legacyDir := filepath.Join(tmpDir, "var", "cloud-integration")
				if err := os.MkdirAll(legacyDir, 0755); err != nil {
					t.Fatalf("failed to create legacy directory: %v", err)
				}
				configPath := filepath.Join(legacyDir, "cloud-integration.json")
				if err := os.WriteFile(configPath, []byte(validConfig), 0644); err != nil {
					t.Fatalf("failed to write test config: %v", err)
				}
				// Set a different config path so primary and subdirectory checks fail
				env.Set(env.ConfigPathEnvVar, filepath.Join(tmpDir, "nonexistent"))
				return func() {
					env.Set(env.ConfigPathEnvVar, "/var/configs")
				}
			},
			expectedFound: true,
			expectedPath:  "var/cloud-integration/cloud-integration.json", // relative to tmpDir
		},
		{
			name: "config does not exist at any path",
			setupFiles: func(t *testing.T) func() {
				tmpDir := t.TempDir()
				// Set config path but don't create any files
				env.Set(env.ConfigPathEnvVar, filepath.Join(tmpDir, "nonexistent"))
				return func() {
					env.Set(env.ConfigPathEnvVar, "/var/configs")
				}
			},
			expectedFound: false,
		},
		{
			name: "primary path takes precedence over subdirectory path",
			setupFiles: func(t *testing.T) func() {
				tmpDir := t.TempDir()

				// Create config at primary path
				primaryPath := filepath.Join(tmpDir, "cloud-integration.json")
				primaryConfig := `{"aws": [{"athenaTable": "primary"}]}`
				if err := os.WriteFile(primaryPath, []byte(primaryConfig), 0644); err != nil {
					t.Fatalf("failed to write primary config: %v", err)
				}

				// Also create config at subdirectory path
				subDir := filepath.Join(tmpDir, "cloud-integration")
				if err := os.MkdirAll(subDir, 0755); err != nil {
					t.Fatalf("failed to create subdirectory: %v", err)
				}
				subPath := filepath.Join(subDir, "cloud-integration.json")
				subConfig := `{"aws": [{"athenaTable": "subdirectory"}]}`
				if err := os.WriteFile(subPath, []byte(subConfig), 0644); err != nil {
					t.Fatalf("failed to write subdirectory config: %v", err)
				}

				env.Set(env.ConfigPathEnvVar, tmpDir)
				return func() {
					env.Set(env.ConfigPathEnvVar, "/var/configs")
				}
			},
			expectedFound: true,
			expectedPath:  "cloud-integration.json", // Should use primary, not subdirectory
		},
		{
			name: "subdirectory path takes precedence over legacy path",
			setupFiles: func(t *testing.T) func() {
				tmpDir := t.TempDir()

				// Create config at subdirectory path
				subDir := filepath.Join(tmpDir, "cloud-integration")
				if err := os.MkdirAll(subDir, 0755); err != nil {
					t.Fatalf("failed to create subdirectory: %v", err)
				}
				subPath := filepath.Join(subDir, "cloud-integration.json")
				if err := os.WriteFile(subPath, []byte(validConfig), 0644); err != nil {
					t.Fatalf("failed to write subdirectory config: %v", err)
				}

				// Also create config at legacy path
				legacyDir := filepath.Join(tmpDir, "var", "cloud-integration")
				if err := os.MkdirAll(legacyDir, 0755); err != nil {
					t.Fatalf("failed to create legacy directory: %v", err)
				}
				legacyPath := filepath.Join(legacyDir, "cloud-integration.json")
				if err := os.WriteFile(legacyPath, []byte(validConfig), 0644); err != nil {
					t.Fatalf("failed to write legacy config: %v", err)
				}

				env.Set(env.ConfigPathEnvVar, tmpDir)
				return func() {
					env.Set(env.ConfigPathEnvVar, "/var/configs")
				}
			},
			expectedFound: true,
			expectedPath:  "cloud-integration/cloud-integration.json", // Should use subdirectory, not legacy
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cleanup := tt.setupFiles(t)
			defer cleanup()

			mcw := &MultiCloudWatcher{}
			configs := mcw.GetConfigs()

			if tt.expectedFound {
				if configs == nil || len(configs) == 0 {
					t.Errorf("Expected to find configs at path containing '%s', but got nil or empty", tt.expectedPath)
				}
				// Verify we got a valid config
				if len(configs) > 0 && configs[0] == nil {
					t.Error("Expected valid config but got nil config")
				}
			} else {
				if configs != nil && len(configs) > 0 {
					t.Errorf("Expected no configs, but got %d configs", len(configs))
				}
			}
		})
	}
}

// TestMultiCloudWatcher_GetConfigs_InvalidJSON tests that invalid JSON is handled gracefully
func TestMultiCloudWatcher_GetConfigs_InvalidJSON(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "cloud-integration.json")

	// Write invalid JSON
	invalidJSON := `{invalid json content`
	if err := os.WriteFile(configPath, []byte(invalidJSON), 0644); err != nil {
		t.Fatalf("failed to write test config: %v", err)
	}

	env.Set(env.ConfigPathEnvVar, tmpDir)
	defer env.Set(env.ConfigPathEnvVar, "/var/configs")

	mcw := &MultiCloudWatcher{}
	configs := mcw.GetConfigs()

	// Should return nil when JSON is invalid
	if configs != nil {
		t.Errorf("Expected nil configs for invalid JSON, got %v", configs)
	}
}

// TestGetConfigPath tests the exported GetConfigPath function
func TestGetConfigPath(t *testing.T) {
	tests := []struct {
		name       string
		envValue   string
		wantSuffix string
	}{
		{
			name:       "default config path",
			envValue:   "",
			wantSuffix: "/var/configs",
		},
		{
			name:       "custom config path",
			envValue:   "/custom/path",
			wantSuffix: "/custom/path",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.envValue != "" {
				env.Set(env.ConfigPathEnvVar, tt.envValue)
				defer env.Set(env.ConfigPathEnvVar, "/var/configs")
			}

			got := pkgenv.GetConfigPath()
			if got != tt.wantSuffix {
				t.Errorf("GetConfigPath() = %v, want %v", got, tt.wantSuffix)
			}
		})
	}
}
