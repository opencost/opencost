package count

// Description - Checks for the allocation summary of pods for each namespace is the same for a prometheus request
// and allocation/summary API request

import (
	// "fmt"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/opencost/opencost-integration-tests/pkg/api"
	"github.com/opencost/opencost-integration-tests/pkg/prometheus"
	"github.com/pmezard/go-difflib/difflib"
)

func TestQueryAllocationSummary(t *testing.T) {
	apiObj := api.NewAPI()

	testCases := []struct {
		name       string
		window     string
		aggregate  string
		accumulate string
	}{
		{
			name:       "Yesterday",
			window:     "24h",
			aggregate:  "pod",
			accumulate: "false",
		},
	}

	t.Logf("testCases: %v", testCases)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			// API Client
			apiResponse, err := apiObj.GetAllocationSummary(api.AllocationRequest{
				Window:     tc.window,
				Aggregate:  tc.aggregate,
				Accumulate: tc.accumulate,
			})

			if err != nil {
				t.Fatalf("Error while calling Allocation API %v", err)
			}
			if apiResponse.Code != 200 {
				t.Errorf("API returned non-200 code")
			}

			queryEnd := time.Now().UTC().Truncate(time.Hour).Add(time.Hour)
			endTime := queryEnd.Unix()

			// Prometheus Client
			// Want to Run avg(avg_over_time(kube_pod_container_status_running[24h]) != 0) by (container, pod, namespace)
			// Running avg(avg_over_time(kube_pod_container_status_running[24h])) by (container, pod, namespace)
			client := prometheus.NewClient()
			promInput := prometheus.PrometheusInput{
				Metric: "kube_pod_container_status_running",
				// MetricNotEqualTo: "0",
				Function:    []string{"avg_over_time", "avg"},
				QueryWindow: tc.window,
				AggregateBy: []string{"container", "pod", "namespace"},
				Time:        &endTime,
			}

			promResponse, err := client.RunPromQLQuery(promInput, t)

			if err != nil {
				t.Fatalf("Error while calling Prometheus API %v", err)
			}

			// Narrow the Prometheus pod set to pods alive at the query
			// endTime using a 1m-resolution subquery. Without this,
			// pods that were only very briefly running inside the 24h
			// window show up in Prometheus (as their avg_over_time is
			// non-zero) but are absent from /allocation/summary, which
			// only reports pods with coincident usage samples. That is
			// a window-boundary race, not a pod-count bug.
			promAliveInput := prometheus.PrometheusInput{
				Metric:              "kube_pod_container_status_running",
				MetricNotEqualTo:    "0",
				Function:            []string{"avg"},
				AggregateBy:         []string{"container", "pod", "namespace", "node"},
				AggregateWindow:     tc.window,
				AggregateResolution: "1m",
				Time:                &endTime,
			}

			promAliveResponse, err := client.RunPromQLQuery(promAliveInput, t)
			if err != nil {
				t.Fatalf("Error while calling Prometheus API %v", err)
			}

			alivePods := make(map[string]bool)
			for _, metric := range promAliveResponse.Data.Result {
				alivePods[metric.Metric.Pod] = true
			}

			var apiAllocationPodNames []string
			for podName, _ := range apiResponse.Data.Sets[0].Allocations {
				// Synthetic value generated and returned by /allocation and not /prometheus
				if slices.Contains([]string{"prometheus-system-unmounted-pvcs", "network-load-gen-unmounted-pvcs"}, podName) {
					continue
				}

				if !slices.Contains(apiAllocationPodNames, podName) {
					apiAllocationPodNames = append(apiAllocationPodNames, podName)
				}
			}

			var promPodNames []string
			for _, promItem := range promResponse.Data.Result {
				// This pod was down, unable to do it with the query
				if promItem.Value.Value == 0 {
					continue
				}
				// Skip pods that are not alive at the query end time.
				// /allocation/summary only returns pods with usage data
				// in the window, so short-lived pods that were up
				// earlier in the 24h window but not at endTime would
				// otherwise produce spurious mismatches.
				if !alivePods[promItem.Metric.Pod] {
					continue
				}
				if !slices.Contains(promPodNames, promItem.Metric.Pod) {
					promPodNames = append(promPodNames, promItem.Metric.Pod)
				}
			}

			apiAllocationsSummaryCount := len(apiAllocationPodNames)
			promAllocationsSummaryCount := len(promPodNames)

			// sort the string slices
			sort.Strings(promPodNames)
			sort.Strings(apiAllocationPodNames)

			promPodNamesString := strings.Join(promPodNames, "\n")
			apiAllocationPodNamesString := strings.Join(apiAllocationPodNames, "\n")

			// Old version file are Prometheus Results and New Version filea are API Allocation Results
			if apiAllocationsSummaryCount != promAllocationsSummaryCount {
				diff := difflib.UnifiedDiff{
					A:        difflib.SplitLines(promPodNamesString),
					B:        difflib.SplitLines(apiAllocationPodNamesString),
					FromFile: "Original",
					ToFile:   "Current",
					Context:  3,
				}
				podNamesDiff, _ := difflib.GetUnifiedDiffString(diff)
				t.Errorf("[Fail]: Number of Pods from Prometheus(%d) and /allocation/summary (%d) did not match.\n Unified Diff:\n %s", promAllocationsSummaryCount, apiAllocationsSummaryCount, podNamesDiff)
			} else {
				t.Logf("[Pass]: Number of Pods from Promtheus and /allocation/summary Match.")
			}

		})
	}
}
