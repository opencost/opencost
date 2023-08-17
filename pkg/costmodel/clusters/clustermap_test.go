package clusters

import (
	"fmt"
	"testing"
	"time"
)

func Test_clusterInfoQuery(t *testing.T) {
	testCases := map[string]struct {
		testOffset             string
		testRefreshTime        time.Duration
		testClusterFilter      string
		expectClusterInfoQuery string
	}{
		"is refresh time 5m yield right prometheus query": {
			testOffset:             "offset 3h",
			testRefreshTime:        5 * time.Minute,
			testClusterFilter:      "",
			expectClusterInfoQuery: fmt.Sprintf("avg_over_time(kubecost_cluster_info{%s}[%s]%s)", "", "5m", "offset 3h"),
		},
		"is cluster filter with id=test working in yield right prometheus query": {
			testOffset:             "",
			testRefreshTime:        5 * time.Minute,
			testClusterFilter:      "id=\"test\"",
			expectClusterInfoQuery: fmt.Sprintf("avg_over_time(kubecost_cluster_info{%s}[%s]%s)", "id=\"test\"", "5m", ""),
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			returnquery := clusterInfoQuery(tc.testOffset, tc.testClusterFilter, tc.testRefreshTime)
			if tc.expectClusterInfoQuery != returnquery {
				t.Fatalf("Case: %s failed to return expected query `%s` but returned `%s`", name, tc.expectClusterInfoQuery, returnquery)
			}
		})
	}
}
