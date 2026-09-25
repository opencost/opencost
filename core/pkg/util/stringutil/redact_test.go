package stringutil

import "testing"

func TestRedactURLs(t *testing.T) {
	cases := map[string]string{
		"plain error": "plain error",
		`Put "https://b.s3.amazonaws.com/k?X-Amz-Signature=abc&X-Amz-Credential=AKIA": EOF`: `Put "https://b.s3.amazonaws.com/k?REDACTED": EOF`,
		"dial https://user:secret@host/path failed":                                         "dial https://REDACTED@host/path failed",
		"see https://host/a#frag and s3://b/k?sig=1":                                        "see https://host/a?REDACTED and s3://b/k?REDACTED",
		"no query https://host/path/object.gz":                                              "no query https://host/path/object.gz",
	}
	for in, want := range cases {
		if got := RedactURLs(in); got != want {
			t.Errorf("RedactURLs(%q) = %q, want %q", in, got, want)
		}
	}
}
