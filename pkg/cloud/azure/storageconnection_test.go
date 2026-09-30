package azure

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/env"
	pkgenv "github.com/opencost/opencost/pkg/env"
)

func TestDeleteFilesOlderThanRetention(t *testing.T) {
	testCases := []struct {
		name     string
		pre      func()
		files    map[string]time.Duration
		deleted  int
		expected string
	}{
		{
			name: "Ensure the default value of '2' works",
			files: map[string]time.Duration{
				"today.gz":        1 * 24 * time.Hour,
				"yesterday.gz":    1.5 * 24 * time.Hour,
				"two_days_ago.gz": 3 * 24 * time.Hour,
			},
			deleted:  1,
			expected: "today.gz,yesterday.gz",
		},
		{
			name: "Ensure the a value of 7 works",
			pre: func() {
				env.Set(pkgenv.CloudCostPvRetentionEnvVar, "7")
			},
			files: map[string]time.Duration{
				"today.gz":        1 * 24 * time.Hour,
				"yesterday.gz":    1.5 * 24 * time.Hour,
				"two_days_ago.gz": 3 * 24 * time.Hour,
			},
			deleted:  0,
			expected: "today.gz,yesterday.gz,two_days_ago.gz",
		},
		{
			name: "Ensure the a value of 7 works",
			pre: func() {
				env.Set(pkgenv.CloudCostPvRetentionEnvVar, "7")
			},
			files: map[string]time.Duration{
				"today.gz":        1 * 24 * time.Hour,
				"yesterday.gz":    1.5 * 24 * time.Hour,
				"two_days_ago.gz": 3 * 24 * time.Hour,
				"old_file.gz":     8 * 24 * time.Hour,
			},
			deleted:  1,
			expected: "today.gz,yesterday.gz,two_days_ago.gz",
		},
	}
	for _, tt := range testCases {
		if tt.pre != nil {
			tt.pre()
		}
		tmpDir, err := os.MkdirTemp("", "test-delete-files")
		if err != nil {
			t.Errorf("Failed to make temp directory: %v", err)
		}
		defer os.RemoveAll(tmpDir)
		for name, days := range tt.files {
			confPath := filepath.Join(tmpDir, name)
			err = os.WriteFile(confPath, []byte(`{"status": "ok"}`), 0644)
			if err != nil {
				t.Errorf("Failed to write file inside temp directory: %v", err)
			}
			modTime := time.Now().Add(-days)
			err = os.Chtimes(confPath, modTime, modTime)
			if err != nil {
				t.Errorf("Failed to set modification time for file: %v", err)
			}
		}

		sc := &StorageConnection{}
		cleaned, err := sc.deleteFilesOlderThanRetention(tmpDir)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(cleaned) != tt.deleted {
			t.Errorf("deleteFilesOlderThanRetention() cleaned %d files, want %d", len(cleaned), tt.deleted)
		}
	}
}
