package configrbac

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/julienschmidt/httprouter"
)

type testSubjectVerifier struct {
	subject string
	orgRole string
	err     error
}

func (v testSubjectVerifier) SubjectFromRequest(*http.Request) (string, error) {
	if v.err != nil {
		return "", v.err
	}
	return v.subject, nil
}

func (v testSubjectVerifier) AuthInfoFromRequest(*http.Request) (UserAuthInfo, error) {
	if v.err != nil {
		return UserAuthInfo{}, v.err
	}
	return UserAuthInfo{Subject: v.subject, OrgRole: v.orgRole}, nil
}

const emptyScopedViewBuckets = `"users":{"availableFor":[],"enforcedFor":[],"enabledByDefaultFor":[],"strictlyEnabledFor":[]},` +
	`"applyToNewUsers":{"availableFor":false,"enforcedFor":false,"enabledByDefaultFor":false,"strictlyEnabledFor":false}`

func TestHandler_DisabledReturns501(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, configFileName)
	if err := os.WriteFile(configPath, []byte(`{"rbac":{"scopedViews":{"enabled":false}}}`), 0o644); err != nil {
		t.Fatal(err)
	}

	loader := &ConfigLoader{path: configPath}
	h := NewHandler(NewService(loader, NewStoreAt(filepath.Join(dir, "scoped_views.db"))))

	tests := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{"get", http.MethodGet, "/config/rbac/scopedViews", ""},
		{"post", http.MethodPost, "/config/rbac/scopedViews", `{"id":"x","name":"X","filters":[],` + emptyScopedViewBuckets + `}`},
		{"put", http.MethodPut, "/config/rbac/scopedViews/x", `{"name":"X","filters":[],` + emptyScopedViewBuckets + `}`},
		{"delete", http.MethodDelete, "/config/rbac/scopedViews/x", ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var req *http.Request
			if tc.body != "" {
				req = httptest.NewRequest(tc.method, tc.path, bytes.NewBufferString(tc.body))
			} else {
				req = httptest.NewRequest(tc.method, tc.path, nil)
			}
			rec := httptest.NewRecorder()
			switch tc.method {
			case http.MethodGet:
				h.GetScopedViews(rec, req, httprouter.Params{})
			case http.MethodPost:
				h.PostScopedView(rec, req, httprouter.Params{})
			case http.MethodPut:
				h.PutScopedView(rec, req, httprouter.Params{httprouter.Param{Key: "id", Value: "x"}})
			case http.MethodDelete:
				h.DeleteScopedView(rec, req, httprouter.Params{httprouter.Param{Key: "id", Value: "x"}})
			}
			if rec.Code != http.StatusNotImplemented {
				t.Fatalf("status = %d, want 501; body=%s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestHandler_EnabledCRUD(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, configFileName)
	dbPath := filepath.Join(dir, "scoped_views.db")
	if err := os.WriteFile(configPath, []byte(`{"rbac":{"scopedViews":{"enabled":true}}}`), 0o644); err != nil {
		t.Fatal(err)
	}

	loader := &ConfigLoader{path: configPath}
	h := NewHandler(NewService(loader, NewStoreAt(dbPath)))

	body := `{
		"id":"scoped-1",
		"name":"Prod",
		"filters":[],
		` + emptyScopedViewBuckets + `
	}`

	req := httptest.NewRequest(http.MethodPost, "/config/rbac/scopedViews", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	h.PostScopedView(rec, req, httprouter.Params{})
	if rec.Code != http.StatusOK {
		t.Fatalf("post status = %d, body=%s", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodPost, "/config/rbac/scopedViews", bytes.NewBufferString(body))
	rec = httptest.NewRecorder()
	h.PostScopedView(rec, req, httprouter.Params{})
	if rec.Code != http.StatusConflict {
		t.Fatalf("duplicate post status = %d, want 409; body=%s", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/config/rbac/scopedViews", nil)
	rec = httptest.NewRecorder()
	h.GetScopedViews(rec, req, httprouter.Params{})
	if rec.Code != http.StatusOK {
		t.Fatalf("list status = %d, body=%s", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/config/rbac/scopedViews/scoped-1", nil)
	rec = httptest.NewRecorder()
	h.GetScopedViews(rec, req, httprouter.Params{httprouter.Param{Key: "id", Value: "scoped-1"}})
	if rec.Code != http.StatusOK {
		t.Fatalf("get status = %d, body=%s", rec.Code, rec.Body.String())
	}

	putBody := bytes.NewBufferString(`{"name":"Prod Updated","filters":[],` + emptyScopedViewBuckets + `}`)
	req = httptest.NewRequest(http.MethodPut, "/config/rbac/scopedViews/scoped-1", putBody)
	rec = httptest.NewRecorder()
	h.PutScopedView(rec, req, httprouter.Params{httprouter.Param{Key: "id", Value: "scoped-1"}})
	if rec.Code != http.StatusOK {
		t.Fatalf("put status = %d, body=%s", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodDelete, "/config/rbac/scopedViews/scoped-1", nil)
	rec = httptest.NewRecorder()
	h.DeleteScopedView(rec, req, httprouter.Params{httprouter.Param{Key: "id", Value: "scoped-1"}})
	if rec.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d, body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandler_UserPolicy(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, configFileName)
	dbPath := filepath.Join(dir, "rbac.db")
	if err := os.WriteFile(configPath, []byte(`{"rbac":{"scopedViews":{"enabled":true}}}`), 0o644); err != nil {
		t.Fatal(err)
	}

	h := NewHandlerWithUserVerifier(
		NewService(&ConfigLoader{path: configPath}, NewStoreAt(dbPath)),
		testSubjectVerifier{subject: "user_abc"},
	)

	viewBody := `{
		"id":"prod-only",
		"name":"Production",
		"filters":[{"id":"f1","dataset":"Billing","field":"namespace","operator":"Equals","value":"prod"}],
		"users":{"availableFor":[],"enforcedFor":["user_abc"],"enabledByDefaultFor":[],"strictlyEnabledFor":[]},
		"applyToNewUsers":{"availableFor":false,"enforcedFor":false,"enabledByDefaultFor":false,"strictlyEnabledFor":false}
	}`
	req := httptest.NewRequest(http.MethodPost, "/config/rbac/scopedViews", bytes.NewBufferString(viewBody))
	rec := httptest.NewRecorder()
	h.PostScopedView(rec, req, httprouter.Params{})
	if rec.Code != http.StatusOK {
		t.Fatalf("create view: %d %s", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/config/rbac/policy/users/user_abc", nil)
	rec = httptest.NewRecorder()
	h.GetUserPolicy(rec, req, httprouter.Params{httprouter.Param{Key: "userId", Value: "user_abc"}})
	if rec.Code != http.StatusOK {
		t.Fatalf("policy: %d %s", rec.Code, rec.Body.String())
	}
	if !bytes.Contains(rec.Body.Bytes(), []byte(`"mode":"enforced"`)) {
		t.Fatalf("expected enforced mode in policy: %s", rec.Body.String())
	}
	if bytes.Contains(rec.Body.Bytes(), []byte(`"roleIds"`)) {
		t.Fatalf("policy should not include role ids: %s", rec.Body.String())
	}
}

func TestHandler_UserPolicyRequiresMatchingAuthenticatedSubject(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, configFileName)
	if err := os.WriteFile(configPath, []byte(`{"rbac":{"scopedViews":{"enabled":true}}}`), 0o644); err != nil {
		t.Fatal(err)
	}

	h := NewHandlerWithUserVerifier(
		NewService(&ConfigLoader{path: configPath}, NewStoreAt(filepath.Join(dir, "rbac.db"))),
		testSubjectVerifier{subject: "user_a"},
	)

	req := httptest.NewRequest(http.MethodGet, "/config/rbac/policy/users/user_b", nil)
	rec := httptest.NewRecorder()
	h.GetUserPolicy(rec, req, httprouter.Params{httprouter.Param{Key: "userId", Value: "user_b"}})
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403; body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandler_PostUserScopedViewRejectsMemberAssigningOtherUsers(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, configFileName)
	if err := os.WriteFile(configPath, []byte(`{"rbac":{"scopedViews":{"enabled":true}}}`), 0o644); err != nil {
		t.Fatal(err)
	}

	h := NewHandlerWithUserVerifier(
		NewService(&ConfigLoader{path: configPath}, NewStoreAt(filepath.Join(dir, "rbac.db"))),
		testSubjectVerifier{subject: "user_a"},
	)

	body := `{
		"id":"personal",
		"name":"Personal",
		"filters":[],
		"users":{"availableFor":["user_b"],"enforcedFor":[],"enabledByDefaultFor":[],"strictlyEnabledFor":[]},
		"applyToNewUsers":{"availableFor":true,"enforcedFor":true,"enabledByDefaultFor":true,"strictlyEnabledFor":true}
	}`
	req := httptest.NewRequest(http.MethodPost, "/config/rbac/users/user_a/scopedViews", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	h.PostUserScopedView(rec, req, httprouter.Params{httprouter.Param{Key: "userId", Value: "user_a"}})
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403; body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandler_PostUserScopedViewPreservesSelectedUsersForOrgAdmin(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, configFileName)
	if err := os.WriteFile(configPath, []byte(`{"rbac":{"scopedViews":{"enabled":true}}}`), 0o644); err != nil {
		t.Fatal(err)
	}

	h := NewHandlerWithUserVerifier(
		NewService(&ConfigLoader{path: configPath}, NewStoreAt(filepath.Join(dir, "rbac.db"))),
		testSubjectVerifier{subject: "user_a", orgRole: "org:admin"},
	)

	body := `{
		"id":"team",
		"name":"Team",
		"filters":[],
		"users":{"availableFor":["user_b"],"enforcedFor":[],"enabledByDefaultFor":[],"strictlyEnabledFor":[]},
		"applyToNewUsers":{"availableFor":true,"enforcedFor":true,"enabledByDefaultFor":true,"strictlyEnabledFor":true}
	}`
	req := httptest.NewRequest(http.MethodPost, "/config/rbac/users/user_a/scopedViews", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	h.PostUserScopedView(rec, req, httprouter.Params{httprouter.Param{Key: "userId", Value: "user_a"}})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}

	policy, err := h.svc.ResolvePolicy("user_a")
	if err != nil {
		t.Fatal(err)
	}
	if len(policy.Views) != 0 {
		t.Fatalf("user_a should not receive view assigned only to user_b: %+v", policy)
	}

	assignedPolicy, err := h.svc.ResolvePolicy("user_b")
	if err != nil {
		t.Fatal(err)
	}
	if len(assignedPolicy.Views) != 1 || assignedPolicy.Views[0].ID != "team" {
		t.Fatalf("user_b should receive selected view: %+v", assignedPolicy)
	}
}

func TestHandler_PostUserScopedViewAllowsMemberPersonalView(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, configFileName)
	if err := os.WriteFile(configPath, []byte(`{"rbac":{"scopedViews":{"enabled":true}}}`), 0o644); err != nil {
		t.Fatal(err)
	}

	h := NewHandlerWithUserVerifier(
		NewService(&ConfigLoader{path: configPath}, NewStoreAt(filepath.Join(dir, "rbac.db"))),
		testSubjectVerifier{subject: "user_a", orgRole: "org:member"},
	)

	body := `{
		"id":"personal",
		"name":"Personal",
		"filters":[],
		"users":{"availableFor":["user_a"],"enforcedFor":[],"enabledByDefaultFor":[],"strictlyEnabledFor":[]},
		"applyToNewUsers":{"availableFor":true,"enforcedFor":true,"enabledByDefaultFor":true,"strictlyEnabledFor":true}
	}`
	req := httptest.NewRequest(http.MethodPost, "/config/rbac/users/user_a/scopedViews", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	h.PostUserScopedView(rec, req, httprouter.Params{httprouter.Param{Key: "userId", Value: "user_a"}})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}

	policy, err := h.svc.ResolvePolicy("user_a")
	if err != nil {
		t.Fatal(err)
	}
	if len(policy.Views) != 1 || policy.Views[0].ID != "personal" {
		t.Fatalf("user_a should receive personal view: %+v", policy)
	}
}

func TestHandler_PostUserScopedViewRejectsAuthFailure(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, configFileName)
	if err := os.WriteFile(configPath, []byte(`{"rbac":{"scopedViews":{"enabled":true}}}`), 0o644); err != nil {
		t.Fatal(err)
	}

	h := NewHandlerWithUserVerifier(
		NewService(&ConfigLoader{path: configPath}, NewStoreAt(filepath.Join(dir, "rbac.db"))),
		testSubjectVerifier{err: errors.New("bad token")},
	)

	body := `{"id":"personal","name":"Personal","filters":[],` + emptyScopedViewBuckets + `}`
	req := httptest.NewRequest(http.MethodPost, "/config/rbac/users/user_a/scopedViews", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	h.PostUserScopedView(rec, req, httprouter.Params{httprouter.Param{Key: "userId", Value: "user_a"}})
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401; body=%s", rec.Code, rec.Body.String())
	}
}

func TestRegisterRoutesDoesNotMountRoleEndpoints(t *testing.T) {
	router := httprouter.New()
	RegisterRoutes(router, nil)

	disallowed := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/config/rbac/roles"},
		{http.MethodPost, "/config/rbac/roles"},
		{http.MethodGet, "/config/rbac/roles/analyst"},
		{http.MethodPut, "/config/rbac/roles/analyst"},
		{http.MethodDelete, "/config/rbac/roles/analyst"},
		{http.MethodGet, "/config/rbac/users/user_abc/roles"},
		{http.MethodPut, "/config/rbac/users/user_abc/roles"},
	}

	for _, route := range disallowed {
		t.Run(route.method+" "+route.path, func(t *testing.T) {
			if handle, _, _ := router.Lookup(route.method, route.path); handle != nil {
				t.Fatalf("route should not be registered")
			}
		})
	}
}

func TestRegisterRoutesProtectsScopedViewManagementReads(t *testing.T) {
	router := httprouter.New()
	protected := map[string]bool{}
	authWrapper := func(next httprouter.Handle) httprouter.Handle {
		return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
			protected[r.Method+" "+r.URL.Path] = true
			next(w, r, ps)
		}
	}
	RegisterRoutes(router, authWrapper)

	tests := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/config/rbac/scopedViews"},
		{http.MethodGet, "/config/rbac/scopedViews/team-a"},
		{http.MethodPost, "/config/rbac/scopedViews"},
		{http.MethodPut, "/config/rbac/scopedViews/team-a"},
		{http.MethodDelete, "/config/rbac/scopedViews/team-a"},
	}

	for _, tc := range tests {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			handle, ps, _ := router.Lookup(tc.method, tc.path)
			if handle == nil {
				t.Fatalf("route not registered")
			}
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			handle(rec, req, ps)
			if !protected[tc.method+" "+tc.path] {
				t.Fatalf("route should be wrapped with admin auth")
			}
		})
	}
}
