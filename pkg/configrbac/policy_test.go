package configrbac

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveViewMode(t *testing.T) {
	view := ScopedView{
		Users: ScopedViewUserBuckets{EnforcedFor: []string{"user_direct"}},
	}

	if got := resolveViewMode(view, "user_direct", false); got != PolicyModeEnforced {
		t.Fatalf("direct user: got %q want enforced", got)
	}

	if got := resolveViewMode(view, "user_other", false); got != "" {
		t.Fatalf("unassigned user: got %q want empty", got)
	}

	newUserView := ScopedView{
		ApplyToNewUsers: ScopedViewApplyNewUsers{EnabledByDefaultFor: true},
	}
	if got := resolveViewMode(newUserView, "user_new", true); got != PolicyModeEnabledByDefault {
		t.Fatalf("new user default: got %q", got)
	}
}

func TestService_ResolvePolicy(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, configFileName)
	if err := os.WriteFile(configPath, []byte(`{"rbac":{"scopedViews":{"enabled":true}}}`), 0o644); err != nil {
		t.Fatal(err)
	}

	store := NewStoreAt(filepath.Join(dir, "test.db"))
	svc := NewService(&ConfigLoader{path: configPath}, store)

	view := ScopedView{
		ID:   "v1",
		Name: "View 1",
		Users: ScopedViewUserBuckets{
			StrictlyEnabledFor: []string{"user_1"},
		},
	}
	if _, err := svc.Create(view); err != nil {
		t.Fatal(err)
	}

	policy, err := svc.ResolvePolicy("user_1")
	if err != nil {
		t.Fatal(err)
	}
	if len(policy.Views) != 1 || policy.Views[0].Mode != PolicyModeStrictlyEnabled {
		t.Fatalf("policy: %+v", policy)
	}
}
