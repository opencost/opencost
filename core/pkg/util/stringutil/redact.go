package stringutil

import (
	"regexp"
)

// urlPattern matches scheme://... URLs up to the next whitespace or quote.
var urlPattern = regexp.MustCompile(`[a-zA-Z][a-zA-Z0-9+.-]*://[^\s"']+`)

// userInfoPattern matches the user:password@ portion following a URL scheme.
var userInfoPattern = regexp.MustCompile(`^([a-zA-Z][a-zA-Z0-9+.-]*://)[^/@]*@`)

// RedactURLs removes query strings, fragments and user info from any URLs contained in s, so
// that error messages from storage clients (which may embed presigned URLs or credentials) are
// safe to expose through status endpoints and diagnostics.
func RedactURLs(s string) string {
	return urlPattern.ReplaceAllStringFunc(s, func(u string) string {
		u = userInfoPattern.ReplaceAllString(u, "${1}REDACTED@")
		for i, r := range u {
			if r == '?' || r == '#' {
				return u[:i] + "?REDACTED"
			}
		}
		return u
	})
}
