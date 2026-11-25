package env

import "testing"

func TestIsPromMtlsAuthEnabled(t *testing.T) {
	t.Run("IsDBmTLSAuthEnabled returns false if all mTLS env vars are not set", func(t *testing.T) {
		got := IsPromMtlsAuthEnabled()
		if got == true {
			t.Errorf("IsDBmTLSAuthEnabled() = %v, want %v", got, false)
		}

		t.Setenv("PROM_MTLS_AUTH_CA_FILE", "some/client.ca")
		got = IsPromMtlsAuthEnabled()
		if got == true {
			t.Errorf("IsDBmTLSAuthEnabled() = %v, want %v", got, false)
		}

		t.Setenv("PROM_MTLS_AUTH_CRT_FILE", "some/client.crt")
		got = IsPromMtlsAuthEnabled()
		if got == true {
			t.Errorf("IsDBmTLSAuthEnabled() = %v, want %v", got, false)
		}

		t.Setenv("PROM_MTLS_AUTH_KEY_FILE", "some/client.key")
		got = IsPromMtlsAuthEnabled()
		if got == false {
			t.Errorf("IsDBmTLSAuthEnabled() = %v, want %v", got, true)
		}
	})
}

// TestIsPrometheusDisableHTTP2 verifies the env var defaults to false and
// can be enabled by setting PROMETHEUS_DISABLE_HTTP2=true.
func TestIsPrometheusDisableHTTP2(t *testing.T) {
	t.Run("defaults to false when env var is not set", func(t *testing.T) {
		got := IsPrometheusDisableHTTP2()
		if got != false {
			t.Errorf("IsPrometheusDisableHTTP2() = %v, want false", got)
		}
	})

	t.Run("returns true when PROMETHEUS_DISABLE_HTTP2=true", func(t *testing.T) {
		t.Setenv(PrometheusDisableHTTP2EnvVar, "true")
		got := IsPrometheusDisableHTTP2()
		if got != true {
			t.Errorf("IsPrometheusDisableHTTP2() = %v, want true", got)
		}
	})

	t.Run("returns false when PROMETHEUS_DISABLE_HTTP2=false", func(t *testing.T) {
		t.Setenv(PrometheusDisableHTTP2EnvVar, "false")
		got := IsPrometheusDisableHTTP2()
		if got != false {
			t.Errorf("IsPrometheusDisableHTTP2() = %v, want false", got)
		}
	})
}
