package httputil

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestBoundedClientHasTotalTimeout(t *testing.T) {
	c := BoundedClient()
	if c.Timeout != PricingTimeout {
		t.Fatalf("expected total timeout %v, got %v", PricingTimeout, c.Timeout)
	}
}

func TestBoundedClientIsShared(t *testing.T) {
	if BoundedClient() != BoundedClient() {
		t.Fatal("expected BoundedClient to return a shared instance")
	}
}

func TestStreamingClientHasNoTotalTimeout(t *testing.T) {
	c := StreamingClient()
	if c.Timeout != 0 {
		t.Fatalf("streaming client must not set a total timeout, got %v", c.Timeout)
	}
}

func TestStreamingClientHasResponseHeaderTimeout(t *testing.T) {
	c := StreamingClient()
	tr, ok := c.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("expected *http.Transport, got %T", c.Transport)
	}
	if tr.ResponseHeaderTimeout != PricingTimeout {
		t.Fatalf("expected response-header timeout %v, got %v", PricingTimeout, tr.ResponseHeaderTimeout)
	}
}

func TestStreamingClientIsShared(t *testing.T) {
	if StreamingClient() != StreamingClient() {
		t.Fatal("expected StreamingClient to return a shared instance")
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

// A base transport that is not a *http.Transport must not panic and must still
// yield a usable client with the response-header timeout applied.
func TestNewStreamingClientFallsBackWhenNotTransport(t *testing.T) {
	// Honor the RoundTripper contract (non-nil response when error is nil), even
	// though this base is only used to exercise the fallback and never round-trips.
	base := roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
	})
	c := newStreamingClient(base)
	tr, ok := c.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("expected fallback *http.Transport, got %T", c.Transport)
	}
	if tr.ResponseHeaderTimeout != PricingTimeout {
		t.Fatalf("expected response-header timeout %v, got %v", PricingTimeout, tr.ResponseHeaderTimeout)
	}
	// The fallback transport must still bound TLS handshake time.
	if tr.TLSHandshakeTimeout == 0 {
		t.Fatal("expected fallback transport to set a TLS handshake timeout")
	}
}

func TestStreamingGetReturnsBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("price-data"))
	}))
	defer srv.Close()

	resp, err := StreamingGet(context.Background(), srv.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading body: %v", err)
	}
	if string(body) != "price-data" {
		t.Fatalf("expected body %q, got %q", "price-data", string(body))
	}
}

func TestStreamingGetHonorsCanceledContext(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before the request runs

	if _, err := StreamingGet(ctx, srv.URL); err == nil {
		t.Fatal("expected an error from a canceled context, got nil")
	}
}

func TestStreamingGetRejectsBadURL(t *testing.T) {
	if _, err := StreamingGet(context.Background(), "://not-a-url"); err == nil {
		t.Fatal("expected an error building the request, got nil")
	}
}

// Cloning must not mutate the shared default transport.
func TestNewStreamingClientClonesWithoutMutatingDefault(t *testing.T) {
	def, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		t.Skip("default transport is not *http.Transport in this environment")
	}
	c := newStreamingClient(def)
	tr := c.Transport.(*http.Transport)
	if tr == def {
		t.Fatal("expected a cloned transport, got the shared default")
	}
	if def.ResponseHeaderTimeout == PricingTimeout {
		t.Fatal("mutated the shared default transport's ResponseHeaderTimeout")
	}
}
