package costmodel

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
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
		name             string
		setToken         string
		authHeader       string
		wantStatus       int
		wantNextCalled   bool
		wantBodySubstr   string
		wantCacheControl string
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

func TestValidateConfigEndpointAuth(t *testing.T) {
	const testToken = "admin-secret-token"

	a := &Accesses{}

	t.Run("503 when admin token not set", func(t *testing.T) {
		t.Setenv(env.AdminTokenEnvVar, "")
		req := httptest.NewRequest(http.MethodGet, "/config/validate", nil)
		rec := httptest.NewRecorder()

		handler := adminAuthMiddleware(a.ValidateConfig)
		handler(rec, req, httprouter.Params{})

		if rec.Code != http.StatusServiceUnavailable {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
		}
	})

	t.Run("401 when auth header missing", func(t *testing.T) {
		t.Setenv(env.AdminTokenEnvVar, testToken)
		req := httptest.NewRequest(http.MethodGet, "/config/validate", nil)
		rec := httptest.NewRecorder()

		handler := adminAuthMiddleware(a.ValidateConfig)
		handler(rec, req, httprouter.Params{})

		if rec.Code != http.StatusUnauthorized {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
		}
	})

	t.Run("403 when wrong bearer token", func(t *testing.T) {
		t.Setenv(env.AdminTokenEnvVar, testToken)
		req := httptest.NewRequest(http.MethodGet, "/config/validate", nil)
		req.Header.Set("Authorization", "Bearer invalid-token")
		rec := httptest.NewRecorder()

		handler := adminAuthMiddleware(a.ValidateConfig)
		handler(rec, req, httprouter.Params{})

		if rec.Code != http.StatusForbidden {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusForbidden)
		}
	})

	t.Run("200 when authorized with valid bearer token", func(t *testing.T) {
		t.Setenv(env.AdminTokenEnvVar, testToken)
		req := httptest.NewRequest(http.MethodGet, "/config/validate", nil)
		req.Header.Set("Authorization", "Bearer "+testToken)
		rec := httptest.NewRecorder()

		handler := adminAuthMiddleware(a.ValidateConfig)
		handler(rec, req, httprouter.Params{})

		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
		}

		var resp map[string]interface{}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to unmarshal JSON response: %v", err)
		}

		if _, ok := resp["data"]; !ok {
			t.Errorf("expected 'data' key in response JSON")
		}
	})
}

func TestValidatePrometheusEndpoint(t *testing.T) {
	t.Run("not configured when empty", func(t *testing.T) {
		res := validatePrometheusEndpoint("")
		if res.Status != "not_configured" {
			t.Errorf("status = %q, want 'not_configured'", res.Status)
		}
	})

	t.Run("connected when server responds 200", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer ts.Close()

		res := validatePrometheusEndpoint(ts.URL)
		if res.Status != "connected" {
			t.Errorf("status = %q, want 'connected'", res.Status)
		}
		if res.Endpoint != ts.URL {
			t.Errorf("endpoint = %q, want %q", res.Endpoint, ts.URL)
		}
	})

	t.Run("unreachable when server responds error", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer ts.Close()

		res := validatePrometheusEndpoint(ts.URL)
		if res.Status != "unreachable" {
			t.Errorf("status = %q, want 'unreachable'", res.Status)
		}
	})
}

func TestValidateCustomPricingCSV(t *testing.T) {
	t.Run("valid CSV parsing", func(t *testing.T) {
		tmpDir := t.TempDir()
		csvFile := filepath.Join(tmpDir, "pricing.csv")
		content := `EndTimestamp,InstanceID,Region,AssetClass,InstanceIDField,InstanceType,MarketPriceHourly,Version
2026-01-01T00:00:00Z,i-12345,us-east-1,node,node.kubernetes.io/instance-type,t3.medium,0.0416,v1
2026-01-01T00:00:00Z,i-67890,us-east-1,node,node.kubernetes.io/instance-type,t3.large,0.0832,v1
`
		if err := os.WriteFile(csvFile, []byte(content), 0644); err != nil {
			t.Fatalf("failed to write test csv: %v", err)
		}

		res := validateCustomPricingCSV(csvFile)
		if !res.Valid {
			t.Errorf("valid = false, want true; error = %s", res.Error)
		}
		if res.RowCount != 2 {
			t.Errorf("rowCount = %d, want 2", res.RowCount)
		}
		if !res.Enabled {
			t.Errorf("enabled = false, want true")
		}
	})

	t.Run("missing CSV file", func(t *testing.T) {
		res := validateCustomPricingCSV("/nonexistent/file.csv")
		if res.Valid {
			t.Errorf("valid = true, want false")
		}
		if res.Error == "" {
			t.Errorf("expected error message for missing CSV file")
		}
	})
}

func TestValidateCustomCostPlugins(t *testing.T) {
	t.Run("disabled when directory absent", func(t *testing.T) {
		t.Setenv("PLUGIN_CONFIG_DIR", "/nonexistent/plugins/config")
		t.Setenv("CUSTOM_COST_ENABLED", "false")

		res := validateCustomCostPlugins()
		if res.Enabled {
			t.Errorf("enabled = true, want false")
		}
		if res.Count != 0 {
			t.Errorf("count = %d, want 0", res.Count)
		}
	})

	t.Run("discovers plugin config files", func(t *testing.T) {
		tmpDir := t.TempDir()
		cfgDir := filepath.Join(tmpDir, "config")
		execDir := filepath.Join(tmpDir, "exec")
		os.MkdirAll(cfgDir, 0755)
		os.MkdirAll(execDir, 0755)

		t.Setenv("PLUGIN_CONFIG_DIR", cfgDir)
		t.Setenv("PLUGIN_EXECUTABLE_DIR", execDir)
		t.Setenv("CUSTOM_COST_ENABLED", "true")

		cfgFile := filepath.Join(cfgDir, "myplugin_config.json")
		if err := os.WriteFile(cfgFile, []byte("{}"), 0644); err != nil {
			t.Fatalf("failed to create config file: %v", err)
		}

		res := validateCustomCostPlugins()
		if !res.Enabled {
			t.Errorf("enabled = false, want true")
		}
		if res.Count != 1 {
			t.Errorf("count = %d, want 1", res.Count)
		}
		if len(res.Plugins) != 1 || res.Plugins[0].Name != "myplugin" {
			t.Errorf("expected plugin 'myplugin', got %+v", res.Plugins)
		}
		if res.Plugins[0].Valid {
			t.Errorf("expected valid = false since executable is missing")
		}
	})
}
