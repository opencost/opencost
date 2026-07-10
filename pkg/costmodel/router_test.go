package costmodel

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/pkg/env"
	"github.com/stretchr/testify/require"
)

func TestAdminAuthMiddleware(t *testing.T) {
	const testToken = "test-admin-token-123"

	nextCalled := false
	next := func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		nextCalled = true
		w.WriteHeader(http.StatusOK)
	}

	tests := []struct {
		name              string
		setToken          string
		authHeader        string
		wantStatus        int
		wantNextCalled    bool
		wantBodySubstr    string
		wantCacheControl  string
	}{
		{
			name:             "no admin token configured - returns 503",
			setToken:         "",
			authHeader:       "",
			wantStatus:       http.StatusServiceUnavailable,
			wantNextCalled:   false,
			wantBodySubstr:   "Admin token is required to activate this endpoint",
			wantCacheControl: "no-store",
		},
		{
			name:             "no admin token configured - bearer ignored, still 503",
			setToken:         "",
			authHeader:       "Bearer anything",
			wantStatus:       http.StatusServiceUnavailable,
			wantNextCalled:   false,
			wantBodySubstr:   "Admin token is required to activate this endpoint",
			wantCacheControl: "no-store",
		},
		{
			name:           "missing authorization header",
			setToken:       testToken,
			authHeader:     "",
			wantStatus:     http.StatusUnauthorized,
			wantNextCalled: false,
		},
		{
			name:           "wrong authorization scheme",
			setToken:       testToken,
			authHeader:     "Basic dXNlcjpwYXNz",
			wantStatus:     http.StatusUnauthorized,
			wantNextCalled: false,
		},
		{
			name:           "bearer with wrong token",
			setToken:       testToken,
			authHeader:     "Bearer wrong-token",
			wantStatus:     http.StatusForbidden,
			wantNextCalled: false,
		},
		{
			name:           "bearer with correct token",
			setToken:       testToken,
			authHeader:     "Bearer " + testToken,
			wantStatus:     http.StatusOK,
			wantNextCalled: true,
		},
		{
			name:           "bearer token with extra spaces after prefix",
			setToken:       testToken,
			authHeader:     "Bearer  " + testToken,
			wantStatus:     http.StatusForbidden,
			wantNextCalled: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setToken != "" {
				t.Setenv(env.AdminTokenEnvVar, tt.setToken)
			} else {
				t.Setenv(env.AdminTokenEnvVar, "")
			}

			nextCalled = false
			req := httptest.NewRequest(http.MethodPost, "/serviceKey", nil)
			if tt.authHeader != "" {
				req.Header.Set("Authorization", tt.authHeader)
			}
			rec := httptest.NewRecorder()

			handler := adminAuthMiddleware(next)
			handler(rec, req, httprouter.Params{})

			if rec.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d", rec.Code, tt.wantStatus)
			}
			if nextCalled != tt.wantNextCalled {
				t.Errorf("nextCalled = %v, want %v", nextCalled, tt.wantNextCalled)
			}
			if tt.wantBodySubstr != "" && !strings.Contains(rec.Body.String(), tt.wantBodySubstr) {
				t.Errorf("body = %q, want substring %q", rec.Body.String(), tt.wantBodySubstr)
			}
			if tt.wantCacheControl != "" && rec.Header().Get("Cache-Control") != tt.wantCacheControl {
				t.Errorf("Cache-Control = %q, want %q", rec.Header().Get("Cache-Control"), tt.wantCacheControl)
			}
		})
	}
}

// newCostDataModelRequest builds a GET request to /costDataModel with the
// given timeWindow query param.
func newCostDataModelRequest(window string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/costDataModel", nil)
	if window != "" {
		q := r.URL.Query()
		q.Set("timeWindow", window)
		r.URL.RawQuery = q.Encode()
	}
	return r
}

func TestCostDataModel_InvalidWindow_Returns500(t *testing.T) {
	tests := []struct {
		name   string
		window string
	}{
		// Parsing failures (err != nil)
		{name: "unparseable", window: "notaduration"},
		{name: "missing", window: ""},
		// Semantic validation failures (DurationString == "")
		{name: "zero minutes", window: "0m"},
		{name: "negative", window: "-1h"},
		{name: "sub-second", window: "1ms"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &Accesses{Model: nil}
			w := httptest.NewRecorder()
			a.CostDataModel(w, newCostDataModelRequest(tt.window), httprouter.Params{})
			require.Equal(t, http.StatusInternalServerError, w.Code)
		})
	}
}
