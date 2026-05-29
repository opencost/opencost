package autocomplete

import (
	"errors"
	"testing"
)

func TestSanitizeSearch(t *testing.T) {
	if got := SanitizeSearch("  ec2  "); got != "ec2" {
		t.Fatalf("SanitizeSearch() = %q, want %q", got, "ec2")
	}
}

func TestNormalizeLimit(t *testing.T) {
	got, err := NormalizeLimit(0)
	if err != nil || got != DefaultResultLimit {
		t.Fatalf("NormalizeLimit(0) = %d, %v", got, err)
	}
	_, err = NormalizeLimit(MaxResultLimit + 1)
	if err == nil || !errors.Is(err, ErrBadRequest) {
		t.Fatalf("expected ErrBadRequest, got %v", err)
	}
}

func TestFilterBySearch(t *testing.T) {
	got := FilterBySearch([]string{"AmazonEC2"}, "ec2")
	if len(got) != 1 || got[0] != "AmazonEC2" {
		t.Fatalf("FilterBySearch() = %v", got)
	}
}
