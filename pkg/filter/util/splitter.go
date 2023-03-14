package util

import (
	"unicode"
)

type UnicodeType uint8

const (
	UnicodeTypeNone UnicodeType = iota
	UnicodeTypeLower
	UnicodeTypeUpper
	UnicodeTypeDigit
	UnicodeTypeOther
)

func unicodeTypeFor(r rune) UnicodeType {
	if unicode.IsLower(r) {
		return UnicodeTypeLower
	}
	if unicode.IsUpper(r) {
		return UnicodeTypeUpper
	}
	if unicode.IsDigit(r) {
		return UnicodeTypeDigit
	}
	return UnicodeTypeOther
}

// SplitPropertyName looks for changes in unicode characters as markers for splitting, and
// and returns only upper and lower case variants. For example:
// "test_case", "test-case", "testCase" would all result in: []string{"test", "case"}
//
// Any character not lowercase or uppercase _will_ cause a split, but will not appear in the
// resulting output. For example:
// "abc123def" would result in: []string{"abc", "def"}
//
// This func also handles cases with repetitive upper case, for example:
// "allocationETLString" would result in: []string{"allocation", "ETL", "String"}
func SplitPropertyName(s string) []string {
	var input []rune = []rune(s)

	var buckets [][]rune
	last := UnicodeTypeNone

	// gather the runes in buckets if the unicode type changes
	for _, r := range input {
		t := unicodeTypeFor(r)

		if t == last && (t != UnicodeTypeDigit && t != UnicodeTypeOther) {
			buckets[len(buckets)-1] = append(buckets[len(buckets)-1], r)
		} else if t != UnicodeTypeDigit && t != UnicodeTypeOther {
			buckets = append(buckets, []rune{r})
		}

		last = t
	}

	// Handle upper case sequences to lower case
	for i := 0; i < len(buckets)-1; i++ {
		if unicode.IsUpper(buckets[i][0]) && unicode.IsLower(buckets[i+1][0]) {
			buckets[i+1] = append([]rune{buckets[i][len(buckets[i])-1]}, buckets[i+1]...)
			buckets[i] = buckets[i][:len(buckets[i])-1]
		}
	}

	// conversion from [][]rune to []string
	result := make([]string, 0, len(buckets))
	for _, s := range buckets {
		if len(s) > 0 {
			result = append(result, string(s))
		}
	}

	return result
}
