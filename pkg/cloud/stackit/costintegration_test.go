package stackit

import (
	"testing"
	"time"
)

func TestParsePeriodDateOnly(t *testing.T) {
	defaultStart := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	defaultEnd := time.Date(2026, 1, 31, 0, 0, 0, 0, time.UTC)

	start, end := parsePeriod("2026-01-05", "2026-01-10", defaultStart, defaultEnd)

	expectedStart := time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)
	expectedEnd := time.Date(2026, 1, 11, 0, 0, 0, 0, time.UTC) // inclusive end + 1 day

	if !start.Equal(expectedStart) {
		t.Errorf("start = %v, want %v", start, expectedStart)
	}
	if !end.Equal(expectedEnd) {
		t.Errorf("end = %v, want %v", end, expectedEnd)
	}
}

func TestParsePeriodRFC3339(t *testing.T) {
	defaultStart := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	defaultEnd := time.Date(2026, 1, 31, 0, 0, 0, 0, time.UTC)

	start, end := parsePeriod("2026-01-05T10:00:00Z", "2026-01-10T18:00:00Z", defaultStart, defaultEnd)

	expectedStart := time.Date(2026, 1, 5, 10, 0, 0, 0, time.UTC)
	expectedEnd := time.Date(2026, 1, 10, 18, 0, 0, 0, time.UTC) // RFC3339 used as-is

	if !start.Equal(expectedStart) {
		t.Errorf("start = %v, want %v", start, expectedStart)
	}
	if !end.Equal(expectedEnd) {
		t.Errorf("end = %v, want %v", end, expectedEnd)
	}
}

func TestParsePeriodEmpty(t *testing.T) {
	defaultStart := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	defaultEnd := time.Date(2026, 1, 31, 0, 0, 0, 0, time.UTC)

	start, end := parsePeriod("", "", defaultStart, defaultEnd)

	if !start.Equal(defaultStart) {
		t.Errorf("start = %v, want default %v", start, defaultStart)
	}
	if !end.Equal(defaultEnd) {
		t.Errorf("end = %v, want default %v", end, defaultEnd)
	}
}

func TestParsePeriodInvalidFallsBack(t *testing.T) {
	defaultStart := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	defaultEnd := time.Date(2026, 1, 31, 0, 0, 0, 0, time.UTC)

	start, end := parsePeriod("not-a-date", "also-not", defaultStart, defaultEnd)

	if !start.Equal(defaultStart) {
		t.Errorf("start = %v, want default %v", start, defaultStart)
	}
	if !end.Equal(defaultEnd) {
		t.Errorf("end = %v, want default %v", end, defaultEnd)
	}
}

func TestExtractRegionFromServiceName(t *testing.T) {
	tests := []struct {
		service string
		want    string
	}{
		{"Tiny Server-t1.2-EU01", "eu01"},
		{"Object Storage Premium-EU02", "eu02"},
		{"GPU Server-n2.14d.g1-EU01", "eu01"},
		{"DNS-100-EU01", "eu01"},
		{"Some Future Service-US01", "us01"},
		{"NoRegionSuffix", "eu01"},
	}
	for _, tt := range tests {
		got := extractRegionFromServiceName(tt.service)
		if got != tt.want {
			t.Errorf("extractRegionFromServiceName(%q) = %q, want %q", tt.service, got, tt.want)
		}
	}
}

func TestSelectSTACKITCategory(t *testing.T) {
	tests := []struct {
		service string
		want    string
	}{
		{"Compute Engine", "Compute"},
		{"SKE Cluster", "Compute"},
		{"Block Storage", "Storage"},
		{"Object Store", "Storage"},
		{"Load Balancer", "Network"},
		{"DNS Service", "Network"},
		{"Some Other Service", "Other"},
	}
	for _, tt := range tests {
		got := selectSTACKITCategory(tt.service)
		if got != tt.want {
			t.Errorf("selectSTACKITCategory(%q) = %q, want %q", tt.service, got, tt.want)
		}
	}
}
