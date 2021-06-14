package util

import (
	"fmt"
	"math"
	"math/rand"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

var alpha = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
var alphanumeric = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

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
