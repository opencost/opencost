package autocomplete

import (
	"fmt"
	"sort"
	"strings"
)

// SanitizeSearch trims whitespace from an autocomplete search string.
func SanitizeSearch(search string) string {
	return strings.TrimSpace(search)
}

// NormalizeLimit applies default and maximum limits for autocomplete results.
func NormalizeLimit(limit int) (int, error) {
	if limit <= 0 {
		return DefaultResultLimit, nil
	}
	if limit > MaxResultLimit {
		return 0, fmt.Errorf("%w: exceeded maximum autocomplete result limit of %d", ErrBadRequest, MaxResultLimit)
	}
	return limit, nil
}

// MapValueFold returns the value for key using case-insensitive key matching.
func MapValueFold(values map[string]string, key string) (string, bool) {
	if v, ok := values[key]; ok {
		return v, true
	}
	for k, v := range values {
		if strings.EqualFold(k, key) {
			return v, true
		}
	}
	return "", false
}

// UniqueSortedLimited returns sorted unique strings capped at limit.
func UniqueSortedLimited(values map[string]struct{}, limit int) []string {
	out := make([]string, 0, len(values))
	for v := range values {
		out = append(out, v)
	}
	sort.Strings(out)
	if len(out) > limit {
		return out[:limit]
	}
	return out
}

// FilterBySearch returns values from list that contain search (case-insensitive).
func FilterBySearch(list []string, search string) []string {
	search = SanitizeSearch(search)
	if search == "" {
		out := make([]string, len(list))
		copy(out, list)
		return out
	}
	needle := strings.ToLower(search)
	out := make([]string, 0, len(list))
	for _, value := range list {
		if strings.Contains(strings.ToLower(value), needle) {
			out = append(out, value)
		}
	}
	return out
}

// ToSet converts a string slice to a set map.
func ToSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, v := range values {
		out[v] = struct{}{}
	}
	return out
}
