package util

import (
	"testing"
	"time"
)

func TestDurationOffsetStrings(t *testing.T) {
	dur, off := "", ""

	dur, off = DurationOffsetStrings(0, 0)
	if dur != "" || off != "" {
		t.Fatalf("DurationOffsetStrings: exp (%s %s); act (%s, %s)", "", "", dur, off)
	}

	dur, off = DurationOffsetStrings(24*time.Hour, 0)
	if dur != "1d" || off != "" {
		t.Fatalf("DurationOffsetStrings: exp (%s %s); act (%s, %s)", "1d", "", dur, off)
	}

	dur, off = DurationOffsetStrings(24*time.Hour+5*time.Minute, 0)
	if dur != "1445m" || off != "" {
		t.Fatalf("DurationOffsetStrings: exp (%s %s); act (%s, %s)", "1445m", "", dur, off)
	}

	dur, off = DurationOffsetStrings(25*time.Hour, 5*time.Minute)
	if dur != "25h" || off != "5m" {
		t.Fatalf("DurationOffsetStrings: exp (%s %s); act (%s, %s)", "25h", "5m", dur, off)
	}

	dur, off = DurationOffsetStrings(25*time.Hour, 60*time.Minute)
	if dur != "25h" || off != "1h" {
		t.Fatalf("DurationOffsetStrings: exp (%s %s); act (%s, %s)", "25h", "1h", dur, off)
	}

	dur, off = DurationOffsetStrings(72*time.Hour, 1440*time.Minute)
	if dur != "3d" || off != "1d" {
		t.Fatalf("DurationOffsetStrings: exp (%s %s); act (%s, %s)", "3d", "1d", dur, off)
	}

	dur, off = DurationOffsetStrings(25*time.Hour, 1*time.Second)
	if dur != "25h" || off != "1s" {
		t.Fatalf("DurationOffsetStrings: exp (%s %s); act (%s, %s)", "25h", "1s", dur, off)
	}

	dur, off = DurationOffsetStrings(24*time.Hour+time.Second, 1*time.Second)
	if dur != "86401s" || off != "1s" {
		t.Fatalf("DurationOffsetStrings: exp (%s %s); act (%s, %s)", "86401s", "1s", dur, off)
	}

	// Expect empty strings if durations are negative
	dur, off = DurationOffsetStrings(-25*time.Hour, -1*time.Second)
	if dur != "" || off != "" {
		t.Fatalf("DurationOffsetStrings: exp (%s %s); act (%s, %s)", "", "", dur, off)
	}
}
