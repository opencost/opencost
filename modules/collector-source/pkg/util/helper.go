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

func HashLabelValues(labels map[string]string, keys []string) uint64 {
	h := fnv.New64a()
	for _, k := range keys {
		h.Write([]byte(labels[k]))
		// make sure that ("ab","c") and ("a","bc") hash differently.
		h.Write([]byte{0xff})
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
