package httputil

import (
	"net/http"
	"testing"
)

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
