package currency

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func newTestAllRatesTodayClient(url string) *allRatesTodayClient {
	return &allRatesTodayClient{
		apiKey:     "test-key",
		baseURL:    url,
		httpClient: http.DefaultClient,
	}
}

func TestAllRatesTodayClient_FetchRates(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer test-key" {
			t.Errorf("expected bearer auth, got %q", got)
		}
		if got := r.URL.Query().Get("source"); got != "USD" {
			t.Errorf("expected source=USD, got %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`[
			{"source":"USD","target":"EUR","rate":0.85,"time":"2026-10-22T12:00:00Z"},
			{"source":"USD","target":"GBP","rate":0.73,"time":"2026-10-22T12:00:00Z"}
		]`))
	}))
	defer server.Close()

	rates, err := newTestAllRatesTodayClient(server.URL).FetchRates("USD")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := map[string]float64{"USD": 1.0, "EUR": 0.85, "GBP": 0.73}
	for ccy, rate := range expected {
		if rates[ccy] != rate {
			t.Errorf("expected %s rate %f, got %f", ccy, rate, rates[ccy])
		}
	}
}

func TestAllRatesTodayClient_Errors(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
	}{
		{"unauthorized", http.StatusUnauthorized, `{"error":"Authentication required."}`},
		{"empty list", http.StatusOK, `[]`},
		{"not a list", http.StatusOK, `{"error":"unexpected"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.status)
				w.Write([]byte(tt.body))
			}))
			defer server.Close()

			if _, err := newTestAllRatesTodayClient(server.URL).FetchRates("USD"); err == nil {
				t.Error("expected error")
			}
		})
	}

	if _, err := (&allRatesTodayClient{}).FetchRates("USD"); err == nil {
		t.Error("expected error for empty API key")
	}
}
