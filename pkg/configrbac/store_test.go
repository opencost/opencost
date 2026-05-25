package configrbac

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestStore_LazyCreateAndCRUD(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "scoped_views.db")

	if _, err := os.Stat(dbPath); !os.IsNotExist(err) {
		t.Fatalf("db should not exist before first use: %v", err)
	}

	store := NewStoreAt(dbPath)
	view := ScopedView{
		ID:   "scoped-1",
		Name: "Team A",
		Filters: []ScopedViewFilterRow{
			{ID: "f1", Dataset: "Billing", Field: "namespace", Operator: "Equals", Value: "prod"},
		},
		Users: ScopedViewUserBuckets{
			EnforcedFor: []string{"user@example.com"},
		},
		CreatedAt: "2025-01-01T00:00:00Z",
		UpdatedAt: "2025-01-01T00:00:00Z",
	}

	if err := store.Create(view); err != nil {
		t.Fatalf("create: %v", err)
	}

	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("db file should exist after first use: %v", err)
	}

	got, err := store.Get("scoped-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Name != "Team A" {
		t.Fatalf("got name %q", got.Name)
	}

	view.Name = "Team A Updated"
	view.UpdatedAt = "2025-01-02T00:00:00Z"
	if err := store.Update(view); err != nil {
		t.Fatalf("update: %v", err)
	}

	list, err := store.List()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != 1 || list[0].Name != "Team A Updated" {
		t.Fatalf("unexpected list: %+v", list)
	}

	if err := store.Delete("scoped-1"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	_, err = store.Get("scoped-1")
	if err == nil || !IsNotFound(err) {
		t.Fatalf("expected not found after delete, got %v", err)
	}
}

func TestService_DisabledDoesNotCreateDB(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, configFileName)
	dbPath := filepath.Join(dir, "rbac", "scoped_views.db")

	if err := os.WriteFile(configPath, []byte(`{"rbac":{"scopedViews":{"enabled":false}}}`), 0o644); err != nil {
		t.Fatal(err)
	}

	loader := &ConfigLoader{path: configPath}
	svc := NewService(loader, NewStoreAt(dbPath))

	_, err := svc.List()
	if !errors.Is(err, ErrScopedViewsDisabled) {
		t.Fatalf("expected disabled error, got %v", err)
	}

	if _, err := os.Stat(dbPath); !os.IsNotExist(err) {
		t.Fatalf("db should not be created when disabled: %v", err)
	}
}

func TestStore_UpdateNotFound(t *testing.T) {
	store := NewStoreAt(filepath.Join(t.TempDir(), "scoped_views.db"))
	err := store.Update(ScopedView{ID: "missing", Name: "x", CreatedAt: "t", UpdatedAt: "t"})
	if err == nil || !IsNotFound(err) {
		t.Fatalf("expected not found, got %v", err)
	}
}
