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
	got, err = NormalizeLimit(50)
	if err != nil || got != 50 {
		t.Fatalf("NormalizeLimit(50) = %d, %v", got, err)
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
	got = FilterBySearch([]string{"AmazonEC2", "S3"}, "")
	if len(got) != 2 {
		t.Fatalf("FilterBySearch empty search = %v", got)
	}
}

func TestMapValueFold(t *testing.T) {
	values := map[string]string{"Team": "platform"}
	if got, ok := MapValueFold(values, "Team"); !ok || got != "platform" {
		t.Fatalf("MapValueFold exact match = %q, %v", got, ok)
	}
	if got, ok := MapValueFold(values, "team"); !ok || got != "platform" {
		t.Fatalf("MapValueFold() = %q, %v", got, ok)
	}
	if _, ok := MapValueFold(values, "missing"); ok {
		t.Fatal("expected missing key")
	}
}

func TestUniqueSortedLimited(t *testing.T) {
	set := map[string]struct{}{"b": {}, "a": {}, "c": {}}
	got := UniqueSortedLimited(set, 2)
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("UniqueSortedLimited() = %v", got)
	}
	all := UniqueSortedLimited(map[string]struct{}{"z": {}, "y": {}}, 5)
	if len(all) != 2 || all[0] != "y" || all[1] != "z" {
		t.Fatalf("UniqueSortedLimited no truncate = %v", all)
	}
}

func TestToSet(t *testing.T) {
	set := ToSet([]string{"a", "b", "a"})
	if len(set) != 2 {
		t.Fatalf("ToSet() = %v", set)
	}
}

func TestIsBadRequest(t *testing.T) {
	if !IsBadRequest(ErrBadRequest) {
		t.Fatal("expected ErrBadRequest")
	}
	if IsBadRequest(errors.New("other")) {
		t.Fatal("expected false for unrelated error")
	}
}
