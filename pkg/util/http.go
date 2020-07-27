package util

import (
	"fmt"
	"net/http"
	"strings"
)

// HeaderString write the request/response http.Header to a string.
func HeaderString(h http.Header) string {
	var sb strings.Builder
	var first bool = true
	sb.WriteString("{ ")

	for k, vs := range h {
		if first {
			first = false
		} else {
			sb.WriteString(", ")
		}
		fmt.Fprintf(&sb, "%s: [ ", k)
		for idx, v := range vs {
			sb.WriteString(v)
			if idx != len(vs)-1 {
				sb.WriteString(", ")
			}
		}
		sb.WriteString(" ]")
	}
	sb.WriteString(" }")

	return sb.String()
}
