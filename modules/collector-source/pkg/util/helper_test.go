package util

import "testing"

func TestHashLabelValues(t *testing.T) {
	keys := []string{"a", "b"}
	tests := []struct {
		name  string
		x, y  map[string]string
		equal bool
	}{
		{"same values", map[string]string{"a": "ab", "b": "c"}, map[string]string{"a": "ab", "b": "c", "extra": "ignored"}, true},
		{"missing key is empty value", map[string]string{"a": "x"}, map[string]string{"a": "x", "b": ""}, true},
		{"shifted boundary", map[string]string{"a": "ab", "b": "c"}, map[string]string{"a": "a", "b": "bc"}, false},
		{"empty value swapped", map[string]string{"a": "x", "b": ""}, map[string]string{"a": "", "b": "x"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := HashLabelValues(tt.x, keys) == HashLabelValues(tt.y, keys); got != tt.equal {
				t.Fatalf("hashes equal = %v, want %v", got, tt.equal)
			}
		})
	}
}

func TestHashLabelValuesDoesNotAllocate(t *testing.T) {
	labels := map[string]string{"namespace": "kube-system", "pod": "coredns-abc", "container": "coredns"}
	keys := []string{"namespace", "pod", "container"}
	if allocs := testing.AllocsPerRun(100, func() { HashLabelValues(labels, keys) }); allocs != 0 {
		t.Fatalf("HashLabelValues allocated %v times per call, want 0", allocs)
	}
}
