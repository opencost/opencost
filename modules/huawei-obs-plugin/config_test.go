package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "huaweiobs_config.json")
	if err := os.WriteFile(path, []byte(`{"region":"la-south-2","buckets":["a","b"]}`), 0o644); err != nil {
		t.Fatalf("writing fixture config: %v", err)
	}

	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Region != "la-south-2" {
		t.Fatalf("expected region la-south-2, got %s", cfg.Region)
	}
	if len(cfg.Buckets) != 2 {
		t.Fatalf("expected 2 buckets, got %d", len(cfg.Buckets))
	}
}

func TestLoadConfig_MissingRegion(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "huaweiobs_config.json")
	if err := os.WriteFile(path, []byte(`{}`), 0o644); err != nil {
		t.Fatalf("writing fixture config: %v", err)
	}

	if _, err := loadConfig(path); err == nil {
		t.Fatalf("expected error for missing region")
	}
}

func TestLoadConfig_MissingFile(t *testing.T) {
	if _, err := loadConfig(filepath.Join(t.TempDir(), "does-not-exist.json")); err == nil {
		t.Fatalf("expected error for missing file")
	}
}
