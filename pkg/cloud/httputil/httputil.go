// Package httputil provides shared HTTP clients for cloud pricing ingestion.
//
// The default net/http client has no timeout, so a hung or unreachable provider
// pricing endpoint can block pricing refresh indefinitely. These helpers return
// clients with sensible timeouts. They are shared and reused so a new connection
// pool is not created on every pricing fetch.
package httputil

import (
	"context"
	"net/http"
	"time"
)

// PricingTimeout is applied to pricing HTTP requests. For the bounded client it
// caps the whole request; for the streaming client it caps the wait for
// response headers only (not the body read).
const PricingTimeout = 30 * time.Second

var (
	boundedClient   = &http.Client{Timeout: PricingTimeout}
	streamingClient = newStreamingClient(http.DefaultTransport)
)

// BoundedClient returns a shared http.Client with a total request timeout,
// suitable for small or bounded pricing API responses.
func BoundedClient() *http.Client {
	return boundedClient
}

// StreamingClient returns a shared http.Client for large pricing downloads (for
// example the AWS pricing file or the Azure price sheet). It bounds the connect,
// TLS handshake, and response-header wait, but not the total body read, so a
// legitimately large or slow download is not truncated while a hung endpoint is
// still abandoned.
func StreamingClient() *http.Client {
	return streamingClient
}

// StreamingGet issues a GET for a large download using the streaming client and
// the caller's context, so the request is cancelable. It centralizes the
// context-aware request construction shared by the large-download paths (the AWS
// pricing file and the Azure price sheet). Callers without a context of their
// own can pass context.Background().
func StreamingGet(ctx context.Context, url string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	return StreamingClient().Do(req)
}

// newStreamingClient builds the streaming client from a base RoundTripper. It
// takes the base transport as a parameter so the fallback path can be exercised
// by tests; production passes http.DefaultTransport.
func newStreamingClient(base http.RoundTripper) *http.Client {
	// Clone the base transport when possible so we keep its dial and TLS
	// handshake timeouts. Guard the type assertion: http.DefaultTransport is
	// declared as a RoundTripper and can be replaced (e.g. in tests), in which
	// case we fall back to a fresh transport rather than panicking.
	transport, ok := base.(*http.Transport)
	if ok {
		transport = transport.Clone()
	} else {
		transport = &http.Transport{}
	}
	transport.ResponseHeaderTimeout = PricingTimeout
	return &http.Client{Transport: transport}
}
