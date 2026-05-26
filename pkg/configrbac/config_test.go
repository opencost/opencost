package configrbac

import (
	"os"
	"path/filepath"
	"testing"
)

func TestConfigLoader_ScopedViewsEnabled(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, configFileName)

	loader := &ConfigLoader{path: path}

	enabled, err := loader.ScopedViewsEnabled()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if enabled {
		t.Fatal("expected disabled when config file is missing")
	}

	cfg := `{"rbac":{"scopedViews":{"enabled":true}}}`
	if err := os.WriteFile(path, []byte(cfg), 0o644); err != nil {
		t.Fatal(err)
	}

	enabled, err = loader.ScopedViewsEnabled()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !enabled {
		t.Fatal("expected enabled from config.json")
	}
}
