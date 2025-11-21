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
