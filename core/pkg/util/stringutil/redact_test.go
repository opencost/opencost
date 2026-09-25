package stringutil

import (
	"strings"
	"testing"
)

func TestRedactURLs(t *testing.T) {
	cases := map[string]string{
		"plain error": "plain error",
		`Put "https://b.s3.amazonaws.com/k?X-Amz-Signature=abc&X-Amz-Credential=AKIA": EOF`: `Put "https://b.s3.amazonaws.com/k?REDACTED": EOF`,
		"dial https://user:secret@host/path failed":                                         "dial https://REDACTED@host/path failed",
		"dial https://user:p@ss@host/path failed":                                           "dial https://REDACTED@host/path failed",
		"see https://host/a#frag and s3://b/k?sig=1":                                        "see https://host/a?REDACTED and s3://b/k?REDACTED",
		"no query https://host/path/object.gz":                                              "no query https://host/path/object.gz",
		"Put bucket.s3.amazonaws.com/k?X-Amz-Signature=abc&x=1":                             "Put bucket.s3.amazonaws.com/k?X-Amz-Signature=REDACTED&x=1",
		`https:\/\/h\/k?sv=2020&sig=SECRET`:                                                 `https:\/\/h\/k?sv=2020&sig=REDACTED`,
		"AccountName=a;AccountKey=SECRET;EndpointSuffix=core":                               "AccountName=a;AccountKey=REDACTED;EndpointSuffix=core",
		"SharedAccessSignature=SECRET;BlobEndpoint=x":                                       "SharedAccessSignature=REDACTED;BlobEndpoint=x",
		"https://h/k?a=1 &sig=SECRET":                                                       "https://h/k?REDACTED &sig=REDACTED",
		"https://storage.googleapis.com/b/o?X-Goog-Signature=SECRET":                        "https://storage.googleapis.com/b/o?REDACTED",
	}
	for in, want := range cases {
		got := RedactURLs(in)
		if got != want {
			t.Errorf("RedactURLs(%q) = %q, want %q", in, got, want)
		}
		if strings.Contains(got, "SECRET") {
			t.Errorf("RedactURLs(%q) leaked a secret: %q", in, got)
		}
	}
}
