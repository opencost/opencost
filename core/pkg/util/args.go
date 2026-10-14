package util

import (
	"strings"

	v1 "k8s.io/api/core/v1"
)

// ParseContainerArgs extracts "--key=value" style arguments from the given containers into a
// flat map. Bare flags without a "=" (e.g. "--verbose") are recorded with an empty value. Args
// are merged across all containers; on key collision, the last container/arg seen wins.
func ParseContainerArgs(containers []v1.Container) map[string]string {
	args := make(map[string]string)
	for _, container := range containers {
		for _, arg := range container.Args {
			trimmed := strings.TrimLeft(arg, "-")
			if trimmed == "" {
				continue
			}
			key, value, _ := strings.Cut(trimmed, "=")
			args[key] = value
		}
	}
	return args
}
