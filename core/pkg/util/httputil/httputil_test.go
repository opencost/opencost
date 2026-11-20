package httputil

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestInvalidKeys(t *testing.T) {
	vals := url.Values{}
	vals.Set("window", "7d")
	vals.Set("aggregate", "namespace")
	vals.Set("filterClsuters", "cluster-two") // Intentional typo

	qp := NewQueryParams(vals)

	result := qp.InvalidKeys([]string{"window", "aggregate", "filterClusters", "filterNamespaces"})
	expected := []string{"filterClsuters"}
	if diff := cmp.Diff(result, expected); len(diff) > 0 {
		t.Errorf("Expected: %+v. Got: %+v", expected, result)
	}
}

func TestHeaderString(t *testing.T) {
	h := make(http.Header)
	h.Add("foo", "abc")
	h.Add("foo", "123")
	h.Add("bar", "foo")
	h.Add("Content-Type", "application/octet-stream")

	s := HeaderString(h)
	if len(s) == 0 {
		t.Errorf("Header String failed to produce a valid output")
		return
	}

	t.Logf("Result: %s\n", s)
}

func TestEmptyHeader(t *testing.T) {
	h := make(http.Header)

	s := HeaderString(h)
	if len(s) == 0 {
		t.Errorf("Header String failed to produce a valid output")
		return
	}

	t.Logf("Result: %s\n", s)
}

func TestNilHeader(t *testing.T) {
	var h http.Header

	s := HeaderString(h)
	if len(s) == 0 {
		t.Errorf("Header String failed to produce a valid output")
		return
	}

	t.Logf("Result: %s\n", s)
}

func TestRateLimitedRetryForUsesRetryAfter(t *testing.T) {
	const defaultWait = 100 * time.Millisecond

	tests := map[string]struct {
		header http.Header
		min    time.Duration
		max    time.Duration
	}{
		"seconds": {
			header: http.Header{"Retry-After": []string{"2"}},
			min:    2 * time.Second,
			max:    2 * time.Second,
		},
		"http date in the future": {
			header: http.Header{"Retry-After": []string{time.Now().Add(30 * time.Second).UTC().Format(http.TimeFormat)}},
			min:    20 * time.Second,
			max:    30 * time.Second,
		},
		"http date in the past": {
			header: http.Header{"Retry-After": []string{time.Now().Add(-30 * time.Second).UTC().Format(http.TimeFormat)}},
			min:    0,
			max:    0,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			// the retry count must not matter when the server says how long to wait
			got := RateLimitedRetryFor(&http.Response{Header: tc.header}, defaultWait, 3)
			if got < tc.min || got > tc.max {
				t.Errorf("expected a wait between %s and %s, got %s", tc.min, tc.max, got)
			}
		})
	}
}

func TestRateLimitedRetryForBacksOffWithoutUsableRetryAfter(t *testing.T) {
	const defaultWait = 100 * time.Millisecond

	// Every one of these leaves the caller without a wait time from the
	// server, so the wait must grow with each retry: 100ms, 200ms, 400ms, ...
	headers := map[string]http.Header{
		"nil header":         nil,
		"empty header":       {},
		"other headers only": {"Content-Type": []string{"text/plain"}},
		"unparseable value":  {"Retry-After": []string{"soon"}},
	}

	for name, header := range headers {
		t.Run(name, func(t *testing.T) {
			for retry := 0; retry < 5; retry++ {
				expected := ExponentialBackoffWaitFor(defaultWait, retry)
				got := RateLimitedRetryFor(&http.Response{Header: header}, defaultWait, retry)
				if got != expected {
					t.Errorf("retry %d: expected %s, got %s", retry, expected, got)
				}
			}
		})
	}
}

// A response read off the wire always has a non-nil Header, even when the
// server sent no Retry-After, so the backoff must not depend on a nil Header.
func TestRateLimitedRetryForRealResponseWithoutRetryAfter(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()

	resp, err := http.Get(server.URL)
	if err != nil {
		t.Fatalf("request failed: %s", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if !IsRateLimited(resp, nil) {
		t.Fatalf("expected a 429 to be treated as rate limited")
	}

	const defaultWait = 100 * time.Millisecond
	for retry, expected := range []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		1600 * time.Millisecond,
	} {
		got := RateLimitedRetryFor(resp, defaultWait, retry)
		if got != expected {
			t.Errorf("retry %d: expected %s, got %s", retry, expected, got)
		}
	}
}
