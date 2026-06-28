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

	explicitUserView := ScopedView{
		Users:           ScopedViewUserBuckets{AvailableFor: []string{"user_owner"}},
		ApplyToNewUsers: ScopedViewApplyNewUsers{AvailableFor: true},
	}
	if got := resolveViewMode(explicitUserView, "user_other", true); got != "" {
		t.Fatalf("explicit-user view should not apply to other new users: got %q", got)
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

func TestService_ResolvePolicyDoesNotReturnOtherUsersExplicitViews(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, configFileName)
	if err := os.WriteFile(configPath, []byte(`{"rbac":{"scopedViews":{"enabled":true}}}`), 0o644); err != nil {
		t.Fatal(err)
	}

	store := NewStoreAt(filepath.Join(dir, "test.db"))
	svc := NewService(&ConfigLoader{path: configPath}, store)

	// Insert directly through the store to mimic older records that still have
	// applyToNewUsers set even though explicit user buckets are present.
	if err := store.Create(ScopedView{
		ID:        "team-a",
		Name:      "Team A",
		CreatedAt: nowRFC3339(),
		UpdatedAt: nowRFC3339(),
		Users: ScopedViewUserBuckets{
			AvailableFor: []string{"user_a"},
		},
		ApplyToNewUsers: ScopedViewApplyNewUsers{AvailableFor: true},
	}); err != nil {
		t.Fatal(err)
	}

	userAPolicy, err := svc.ResolvePolicy("user_a")
	if err != nil {
		t.Fatal(err)
	}
	if len(userAPolicy.Views) != 1 || userAPolicy.Views[0].ID != "team-a" {
		t.Fatalf("user_a policy: %+v", userAPolicy)
	}

	userXPolicy, err := svc.ResolvePolicy("user_x")
	if err != nil {
		t.Fatal(err)
	}
	if len(userXPolicy.Views) != 0 {
		t.Fatalf("user_x should not receive user_a view: %+v", userXPolicy)
	}
}

func TestService_CreateClearsApplyToNewUsersForExplicitUsers(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, configFileName)
	if err := os.WriteFile(configPath, []byte(`{"rbac":{"scopedViews":{"enabled":true}}}`), 0o644); err != nil {
		t.Fatal(err)
	}

	store := NewStoreAt(filepath.Join(dir, "test.db"))
	svc := NewService(&ConfigLoader{path: configPath}, store)

	created, err := svc.Create(ScopedView{
		ID:   "team-a",
		Name: "Team A",
		Users: ScopedViewUserBuckets{
			AvailableFor: []string{"user_a"},
		},
		ApplyToNewUsers: ScopedViewApplyNewUsers{
			AvailableFor:        true,
			EnforcedFor:         true,
			EnabledByDefaultFor: true,
			StrictlyEnabledFor:  true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if created.ApplyToNewUsers.AvailableFor ||
		created.ApplyToNewUsers.EnforcedFor ||
		created.ApplyToNewUsers.EnabledByDefaultFor ||
		created.ApplyToNewUsers.StrictlyEnabledFor {
		t.Fatalf("applyToNewUsers should be cleared for explicit users: %+v", created.ApplyToNewUsers)
	}

	stored, err := svc.Get("team-a")
	if err != nil {
		t.Fatal(err)
	}
	if stored.ApplyToNewUsers.AvailableFor ||
		stored.ApplyToNewUsers.EnforcedFor ||
		stored.ApplyToNewUsers.EnabledByDefaultFor ||
		stored.ApplyToNewUsers.StrictlyEnabledFor {
		t.Fatalf("stored applyToNewUsers should be cleared for explicit users: %+v", stored.ApplyToNewUsers)
	}
}
