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
	tests := []struct {
		name    string
		limit   int
		want    int
		wantErr bool
	}{
		{name: "default", limit: 0, want: 100},
		{name: "explicit", limit: 25, want: 25},
		{name: "too large", limit: 1001, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NormalizeLimit(tt.limit, 100, 1000)
			if (err != nil) != tt.wantErr {
				t.Fatalf("NormalizeLimit() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				if !errors.Is(err, ErrBadRequest) {
					t.Fatalf("expected ErrBadRequest, got %v", err)
				}
				return
			}
			if got != tt.want {
				t.Fatalf("NormalizeLimit() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestMapValueFold(t *testing.T) {
	values := map[string]string{"App": "frontend"}
	if got, ok := MapValueFold(values, "app"); !ok || got != "frontend" {
		t.Fatalf("MapValueFold() = %q, %v", got, ok)
	}
}

func TestUniqueSortedLimited(t *testing.T) {
	values := map[string]struct{}{"b": {}, "a": {}, "c": {}}
	got := UniqueSortedLimited(values, 2)
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("UniqueSortedLimited() = %v", got)
	}
}

func TestFilterBySearch(t *testing.T) {
	list := []string{"AmazonEC2", "AmazonS3"}
	got := FilterBySearch(list, "ec2")
	if len(got) != 1 || got[0] != "AmazonEC2" {
		t.Fatalf("FilterBySearch() = %v", got)
	}
}
