package stringutil

import (
	"regexp"
	"strings"
)

// urlPattern matches scheme://... URLs up to the next whitespace or quote.
var urlPattern = regexp.MustCompile(`[a-zA-Z][a-zA-Z0-9+.-]*://[^\s"']+`)

// userInfoPattern matches the user:password@ portion following a URL scheme, up to the last @ before
// any query or fragment, so passwords containing @ or / are fully removed. A path containing @ is
// over-redacted, which fails safe.
var userInfoPattern = regexp.MustCompile(`^([a-zA-Z][a-zA-Z0-9+.-]*://)[^?#]*@`)

// secretParamPattern matches the values of well known signature and credential parameters used by
// cloud storage presigned URLs and connection strings, wherever they appear.
// Values may follow = (query strings, connection strings), : (headers) or ":" (JSON).
var secretParamPattern = regexp.MustCompile(`(?i)\b(sig|signature|x-amz-signature|x-amz-credential|x-amz-security-token|x-goog-signature|x-goog-credential|accountkey|sharedaccesssignature|access_token|refresh_token|client_secret)("?\s*[:=]\s*"?)[^&;,\s"']+`)

// genericSecretPattern matches password and token values only after = or a quoted JSON key, so that
// prose such as "failed to get token: ..." is left intact.
var genericSecretPattern = regexp.MustCompile(`(?i)\b(password|token)(\s*=\s*"?|"\s*:\s*")[^&;,\s"']+`)

// RedactURLs removes query strings, fragments and user info from any URLs contained in s, and the
// values of well known signature and credential parameters anywhere in s, so that error messages
// from storage clients (which may embed presigned URLs or credentials) are safe to expose through
// logs, status endpoints and diagnostics.
func RedactURLs(s string) string {
	s = urlPattern.ReplaceAllStringFunc(s, func(u string) string {
		u = userInfoPattern.ReplaceAllString(u, "${1}REDACTED@")
		if i := strings.IndexAny(u, "?#"); i >= 0 {
			return u[:i] + "?REDACTED"
		}
		return u
	})
	s = secretParamPattern.ReplaceAllString(s, "${1}${2}REDACTED")
	return genericSecretPattern.ReplaceAllString(s, "${1}${2}REDACTED")
}
