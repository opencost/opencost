package httpclient

import (
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
)

const (
	defaultMaxRetries    = 5
	defaultRetryBaseWait = 2 * time.Second
	defaultRetryMaxWait  = 60 * time.Second
)

// retryTransport is an http.RoundTripper that retries requests on 429 and 503s
type retryTransport struct {
	wrapped     http.RoundTripper
	maxRetries  int
	baseWait    time.Duration
	maxWait     time.Duration
}

func (t *retryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	wait := t.baseWait
	for attempt := 0; attempt <= t.maxRetries; attempt++ {
		resp, err := t.wrapped.RoundTrip(req)
		if err != nil {
			return nil, err
		}

		if resp.StatusCode != http.StatusTooManyRequests && resp.StatusCode != http.StatusServiceUnavailable {
			return resp, nil
		}

		if attempt == t.maxRetries {
			// Return the final error response untouched so the caller can
			// read the body and status code.
			return resp, nil
		}

		// Consume and discard the error body so the connection can be reused,
		// then close it before sleeping.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()

		delay := wait
		if ra := resp.Header.Get("Retry-After"); ra != "" {
			if secs, err := strconv.Atoi(ra); err == nil {
				delay = time.Duration(secs) * time.Second
			}
		}
		if delay > t.maxWait {
			delay = t.maxWait
		}

		log.Warnf("pricing httpclient: HTTP %d, retrying in %s (attempt %d/%d)",
			resp.StatusCode, delay, attempt+1, t.maxRetries)
		time.Sleep(delay)
		wait *= 2
	}
	return nil, nil
}

// NewClient returns an *http.Client whose transport automatically retries
// on HTTP 429 / 503 with exponential backoff
func NewClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		Transport: &retryTransport{
			wrapped:    http.DefaultTransport,
			maxRetries: defaultMaxRetries,
			baseWait:   defaultRetryBaseWait,
			maxWait:    defaultRetryMaxWait,
		},
	}
}
