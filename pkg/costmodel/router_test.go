package costmodel

import (
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/pkg/env"
)

func TestAdminAuthMiddleware(t *testing.T) {
	const testToken = "test-admin-token-123"

	nextCalled := false
	next := func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		nextCalled = true
		w.WriteHeader(http.StatusOK)
	}

	tests := []struct {
		name           string
		setToken       string
		authHeader     string
		wantStatus     int
		wantNextCalled bool
	}{
		{
			name:           "no admin token configured - request allowed with deduped warning",
			setToken:       "",
			authHeader:     "",
			wantStatus:     http.StatusOK,
			wantNextCalled: true,
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
			prev := os.Getenv(env.AdminTokenEnvVar)
			defer func() {
				if prev == "" {
					os.Unsetenv(env.AdminTokenEnvVar)
				} else {
					os.Setenv(env.AdminTokenEnvVar, prev)
				}
			}()
			if tt.setToken != "" {
				os.Setenv(env.AdminTokenEnvVar, tt.setToken)
			} else {
				os.Unsetenv(env.AdminTokenEnvVar)
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
		})
	}
}
