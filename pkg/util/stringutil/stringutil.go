package stringutil

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"
)

const (
	_ = 1 << (10 * iota)
	// KiB is bytes per Kibibyte
	KiB
	// MiB is bytes per Mebibyte
	MiB
	// GiB is bytes per Gibibyte
	GiB
	// TiB is bytes per Tebibyte
	TiB
)

var alpha = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
var alphanumeric = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

// Any strings created at runtime, duplicate or not, are copied, even though by specification,
// a go string is immutable. This utility allows us to cache runtime strings and retrieve them
// when we expect heavy duplicates.
var strings sync.Map

func init() {
	rand.Seed(time.Now().UnixNano())
}

// Bank will return a non-copy of a string if it has been used before. Otherwise, it will store
// the string as the unique instance.
func Bank(s string) string {
	ss, _ := strings.LoadOrStore(s, s)
	return ss.(string)
}

// RandSeq generates a pseudo-random alphabetic string of the given length
func RandSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = alpha[rand.Intn(len(alpha))] // #nosec No need for a cryptographic strength random here
	}
	return string(b)
}

// FormatBytes takes a number of bytes and formats it as a string
func FormatBytes(numBytes int64) string {
	if numBytes > TiB {
		return fmt.Sprintf("%.2fTiB", float64(numBytes)/TiB)
	}
	if numBytes > GiB {
		return fmt.Sprintf("%.2fGiB", float64(numBytes)/GiB)
	}
	if numBytes > MiB {
		return fmt.Sprintf("%.2fMiB", float64(numBytes)/MiB)
	}
	if numBytes > KiB {
		return fmt.Sprintf("%.2fKiB", float64(numBytes)/KiB)
	}
	return fmt.Sprintf("%dB", numBytes)
}

// FormatUTCOffset converts a duration to a string of format "-07:00"
func FormatUTCOffset(dur time.Duration) string {
	utcOffSig := "+"
	if dur.Hours() < 0 {
		utcOffSig = "-"
	}
	utcOffHrs := int(math.Trunc(math.Abs(dur.Hours())))
	utcOffMin := int(math.Abs(dur.Minutes())) - (utcOffHrs * 60)

	return fmt.Sprintf("%s%02d:%02d", utcOffSig, utcOffHrs, utcOffMin)
}

// StringSlicesEqual checks if two string slices with arbitrary order have the same elements
func StringSlicesEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	// Build maps for each slice that counts each unique instance
	leftMap := make(map[string]int, len(left))
	for _, str := range left {
		count, ok := leftMap[str]; if ok {
			leftMap[str] = count + 1
		} else {
			leftMap[str] = 1
		}
	}
	rightMap := make(map[string]int, len(right))
	for _, str := range right {
		count, ok := rightMap[str]; if ok {
			rightMap[str] = count + 1
		} else {
			rightMap[str] = 1
		}
	}
	// check that each unique key has the same count in each slice
	for key, count := range leftMap {
		if rightMap[key] != count {
			return false
		}
	}
	return true
}
