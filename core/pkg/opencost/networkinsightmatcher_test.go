package opencost

import (
	"testing"

	nfilter "github.com/opencost/opencost/core/pkg/filter/networkinsight"
)

func matchNetworkInsight(t *testing.T, ni *NetworkInsight, filter string) bool {
	t.Helper()

	tree, err := nfilter.NewNetworkInsightFilterParser().Parse(filter)
	if err != nil {
		t.Fatalf("Parse(%q) error = %v", filter, err)
	}

	m, err := NewNetworkInsightMatchCompiler().Compile(tree)
	if err != nil {
		t.Fatalf("Compile(%q) error = %v", filter, err)
	}

	return m.Matches(ni)
}

// The grammar and the field map have to agree. A field the parser accepts but the map cannot reach
// returns "Failed to find string identifier" at match time, not at parse time.
func TestNetworkInsightMatcher_Fields(t *testing.T) {
	ni := &NetworkInsight{
		Cluster:   "cluster-a",
		Namespace: "kubecost",
		Pod:       "pod-1",
	}

	cases := []struct {
		filter string
		want   bool
	}{
		{`cluster:"cluster-a"`, true},
		{`cluster:"cluster-b"`, false},
		{`namespace:"kubecost"`, true},
		{`pod:"pod-1"`, true},
	}

	for _, tc := range cases {
		t.Run(tc.filter, func(t *testing.T) {
			if got := matchNetworkInsight(t, ni, tc.filter); got != tc.want {
				t.Errorf("Matches(%q) = %v, want %v", tc.filter, got, tc.want)
			}
		})
	}
}

// account is in the grammar so it parses and compiles, but the insight does not carry it. The
// matcher must not silently treat that as a match once storage has joined the account in.
func TestNetworkInsightMatcher_AccountIsNotResolvable(t *testing.T) {
	ni := &NetworkInsight{Cluster: "cluster-a", Namespace: "kubecost"}

	for _, filter := range []string{
		`account:"aws-111"`,
		`account:""`,
		`account:"aws-111"+namespace:"kubecost"`,
	} {
		t.Run(filter, func(t *testing.T) {
			if matchNetworkInsight(t, ni, filter) {
				t.Errorf("Matches(%q) = true, want false", filter)
			}
		})
	}
}
