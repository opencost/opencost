package autocomplete

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

// ErrBadRequest indicates a client validation error for autocomplete requests.
var ErrBadRequest = errors.New("autocomplete bad request")

// SanitizeSearch trims whitespace from an autocomplete search string.
func SanitizeSearch(search string) string {
	return strings.TrimSpace(search)
}

// NormalizeLimit applies default and maximum limits for autocomplete results.
func NormalizeLimit(limit, defaultLimit, maxLimit int) (int, error) {
	if limit <= 0 {
		return defaultLimit, nil
	}
	if limit > maxLimit {
		return 0, fmt.Errorf("%w: exceeded maximum autocomplete result limit of %d", ErrBadRequest, maxLimit)
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
// An empty search returns the full list.
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
