package env

import "testing"

func TestIsDBmTLSAuthEnabled(t *testing.T) {
	t.Run("IsDBmTLSAuthEnabled returns false if all mTLS env vars are not set", func(t *testing.T) {
		got := IsDBmTLSAuthEnabled()
		if got == true {
			t.Errorf("IsDBmTLSAuthEnabled() = %v, want %v", got, false)
		}

		t.Setenv("DB_MTLS_AUTH_CA_FILE", "some/client.ca")
		got = IsDBmTLSAuthEnabled()
		if got == true {
			t.Errorf("IsDBmTLSAuthEnabled() = %v, want %v", got, false)
		}

		t.Setenv("DB_MTLS_AUTH_CRT_FILE", "some/client.crt")
		got = IsDBmTLSAuthEnabled()
		if got == true {
			t.Errorf("IsDBmTLSAuthEnabled() = %v, want %v", got, false)
		}

		t.Setenv("DB_MTLS_AUTH_KEY_FILE", "some/client.key")
		got = IsDBmTLSAuthEnabled()
		if got == false {
			t.Errorf("IsDBmTLSAuthEnabled() = %v, want %v", got, true)
		}
	})
}
