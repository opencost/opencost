package source

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func newCommError() error {
	return NewCommError("Test Communication Error")
}

func newErrorCollection() error {
	qc := &QueryErrorCollector{}

	qc.Report("test_query1", nil, NewCommError("Failed to connect"), nil)
	qc.Report("test_query2", nil, NewCommError("Failed to connect"), errors.New("Parsing error"))
	qc.Report("test_query3", nil, nil, errors.New("Failed to parse field 'foo'"))

	return qc
}

func newNestedError() error {
	comErr := NewCommError("Communication Error")
	e1 := fmt.Errorf("Wrap Error #1: %w", comErr)
	e2 := fmt.Errorf("Wrap Error #2: %w", e1)
	return e2
}

func TestErrorCollectionCheck(t *testing.T) {
	err := newErrorCollection()

	if !IsErrorCollection(err) {
		t.Fatalf("IsErrorCollection() returned false, expected true")
		return
	}
}

func TestNestedErrorAs(t *testing.T) {
	err := newNestedError()

	var commErr CommError
	if !errors.As(err, &commErr) {
		t.Fatalf("Expected there to exist a CommError, but failed.")
		return
	}
}

func TestErrorCollectionErrorAs(t *testing.T) {
	err := newErrorCollection()

	var commErr CommError
	if !errors.As(err, &commErr) {
		t.Fatalf("Expected there to exist a CommError, but failed.")
		return
	}
}

func TestCommErrorAs(t *testing.T) {
	err := newCommError()

	var commErr CommError
	if !errors.As(err, &commErr) {
		t.Fatalf("Expected there to exist a CommError, but failed.")
		return
	}
}

// TestCommError464Hint verifies that a CommError containing "464" in its
// message includes the actionable AWS ALB hint so operators know to set
// PROMETHEUS_DISABLE_HTTP2=true.
func TestCommError464Hint(t *testing.T) {
	err := CommErrorf("464 () URL: 'https://prometheus.example.com/api/v1/query'")
	msg := err.Error()

	if !strings.Contains(msg, "PROMETHEUS_DISABLE_HTTP2=true") {
		t.Errorf("Expected 464 error message to contain PROMETHEUS_DISABLE_HTTP2=true hint, got: %s", msg)
	}
	if !strings.Contains(msg, "AWS ALB protocol mismatch") {
		t.Errorf("Expected 464 error message to contain 'AWS ALB protocol mismatch', got: %s", msg)
	}
}

// TestCommErrorNoFalsePositive464Hint verifies that non-464 errors do NOT
// get the AWS ALB hint injected.
func TestCommErrorNoFalsePositive464Hint(t *testing.T) {
	for _, code := range []string{"400", "401", "403", "404", "500", "503"} {
		err := CommErrorf("%s () URL: 'https://prometheus.example.com/api/v1/query'", code)
		msg := err.Error()
		if strings.Contains(msg, "PROMETHEUS_DISABLE_HTTP2") {
			t.Errorf("Non-464 error (code %s) should not contain PROMETHEUS_DISABLE_HTTP2 hint, got: %s", code, msg)
		}
	}
}

func TestAllErrorsFor(t *testing.T) {
	err := newErrorCollection()
	if !IsErrorCollection(err) {
		t.Fatalf("Error is not ErrorCollection")
		return
	}
	collection := err.(QueryErrorCollection)
	allErrors := AllErrorsFor(collection)

	// Expected Errors Length
	const expected = 4

	if len(allErrors) != expected {
		t.Fatalf("All Errors Length was: %d, Expected %d", len(allErrors), expected)
		return
	}
}
