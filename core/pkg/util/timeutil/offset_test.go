package timeutil

import (
	"testing"
	"time"
)

func TestParseUTCOffset(t *testing.T) {
	tests := []struct {
		offset string
		want   time.Duration
	}{
		{offset: "+05:30", want: 5*time.Hour + 30*time.Minute},
		{offset: "-03:30", want: -3*time.Hour - 30*time.Minute},
		{offset: "+05:45", want: 5*time.Hour + 45*time.Minute},
		{offset: "+00:30", want: 30 * time.Minute},
		{offset: "-00:30", want: -30 * time.Minute},
		{offset: "+08:00", want: 8 * time.Hour},
		{offset: "+00:00", want: 0},
		{offset: "", want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.offset, func(t *testing.T) {
			got, err := ParseUTCOffset(tt.offset)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Errorf("ParseUTCOffset(%q) = %s, want %s", tt.offset, got, tt.want)
			}
		})
	}
}
