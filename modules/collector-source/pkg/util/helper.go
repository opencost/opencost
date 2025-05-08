package util

import (
	"hash/fnv"
	"strings"
)

var (
	KB = 1024
	MB = 1024 * KB
	GB = 1024 * MB
)

func Hash(s []string) uint64 {
	h := fnv.New64a()
	for _, v := range s {
		h.Write([]byte(v))
	}
	return h.Sum64()
}

func MetricNameFor(metric string, labels []string, values []string) string {
	var sb strings.Builder
	sb.WriteString(metric)
	sb.WriteRune('{')
	for i := 0; i < len(labels); i++ {
		sb.WriteRune('"')
		sb.WriteString(labels[i])
		sb.WriteString(`"="`)
		sb.WriteString(values[i])
		sb.WriteRune('"')
		if i < len(labels)-1 {
			sb.WriteRune(',')
		}
	}
	sb.WriteRune('}')
	return sb.String()
}

func ToMap(labels []string, values []string) map[string]string {
	min := len(labels)
	if len(values) < min {
		min = len(values)
	}

	m := make(map[string]string, min)
	for i := 0; i < min; i++ {
		m[labels[i]] = values[i]
	}
	return m
}

func Ptr[T any](v T) *T {
	return &v
}
