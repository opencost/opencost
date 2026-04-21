package allocation

// Description
// Check Pod Labels from API Match results from Promethues

import (
	"testing"
	"time"

	"github.com/opencost/opencost-integration-tests/pkg/api"
	"github.com/opencost/opencost-integration-tests/pkg/prometheus"
)

func TestPodLabels(t *testing.T) {
	apiObj := api.NewAPI()

	testCases := []struct {
		name                      string
		window                    string
		aggregate                 string
		accumulate                string
		includeAggregatedMetadata string
	}{
		{
			name:                      "Today",
			window:                    "24h",
			aggregate:                 "pod",
			accumulate:                "true",
			includeAggregatedMetadata: "true",
		},
	}

	t.Logf("testCases: %v", testCases)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			queryEnd := time.Now().UTC().Truncate(time.Hour).Add(time.Hour)
			endTime := queryEnd.Unix()

			// -------------------------------
			// Pod Running Time
			// avg(avg_over_time(kube_pod_container_status_running{%s}[%s])) by (pod)
			// -------------------------------
			client := prometheus.NewClient()
			promPodRunningInfoInput := prometheus.PrometheusInput{}
			promPodRunningInfoInput.Metric = "kube_pod_container_status_running"
			promPodRunningInfoInput.Function = []string{"avg_over_time", "avg"}
			promPodRunningInfoInput.QueryWindow = tc.window
			promPodRunningInfoInput.AggregateBy = []string{"pod"}
			promPodRunningInfoInput.Time = &endTime

			promPodRunningInfo, err := client.RunPromQLQuery(promPodRunningInfoInput, t)
			if err != nil {
				t.Fatalf("Error while calling Prometheus API %v", err)
			}

			podRunningStatus := make(map[string]int)

			for _, promPodRunningInfoItem := range promPodRunningInfo.Data.Result {
				pod := promPodRunningInfoItem.Metric.Pod
				runningStatus := int(promPodRunningInfoItem.Value.Value)

				// kube_pod_labels and kube_nodespace_labels might hold labels for dead pods as well
				// filter the ones that are running because allocation filters for that
				podRunningStatus[pod] = runningStatus
			}

			// Pod Info - narrow the "running" set to pods that were actually
			// running at the query endTime using a 1m resolution subquery,
			// matching the pattern used in pod_annotations_test.go.
			// Pods that only briefly existed earlier in the 24h window may
			// not appear in /allocation, and comparing their labels yields
			// false negatives that have nothing to do with label
			// propagation.
			promPodInfoInput := prometheus.PrometheusInput{}
			promPodInfoInput.Metric = "kube_pod_container_status_running"
			promPodInfoInput.MetricNotEqualTo = "0"
			promPodInfoInput.AggregateBy = []string{"container", "pod", "namespace", "node"}
			promPodInfoInput.Function = []string{"avg"}
			promPodInfoInput.AggregateWindow = tc.window
			promPodInfoInput.AggregateResolution = podStatusResolution
			promPodInfoInput.Time = &endTime

			podInfo, err := client.RunPromQLQuery(promPodInfoInput, t)
			if err != nil {
				t.Fatalf("Error while calling Prometheus API %v", err)
			}

			alive := make(map[string]bool)
			for _, r := range podInfo.Data.Result {
				alive[r.Metric.Pod] = true
			}

			// -------------------------------
			// Pod Labels
			// avg_over_time(kube_pod_labels{%s}[%s])
			// -------------------------------
			promLabelInfoInput := prometheus.PrometheusInput{}
			promLabelInfoInput.Metric = "kube_pod_labels"
			promLabelInfoInput.Function = []string{"avg_over_time"}
			promLabelInfoInput.QueryWindow = tc.window
			promLabelInfoInput.Time = &endTime

			promlabelInfo, err := client.RunPromQLQuery(promLabelInfoInput, t)
			if err != nil {
				t.Fatalf("Error while calling Prometheus API %v", err)
			}

			// Store Results in a Pod Map
			type PodData struct {
				Pod         string
				Alive       bool
				InAlloc     bool
				PromLabels  map[string]string
				AllocLabels map[string]string
			}

			podMap := make(map[string]*PodData)

			// Store Prometheus Pod Prometheus Results
			for _, promlabel := range promlabelInfo.Data.Result {
				pod := promlabel.Metric.Pod
				labels := promlabel.Metric.Labels

				// Skip Dead Pods
				if podRunningStatus[pod] == 0 {
					continue
				}

				podMap[pod] = &PodData{
					Pod:        pod,
					Alive:      alive[pod],
					PromLabels: labels,
				}
			}

			// API Response
			apiResponse, err := apiObj.GetAllocation(api.AllocationRequest{
				Window:                    tc.window,
				Aggregate:                 tc.aggregate,
				Accumulate:                tc.accumulate,
				IncludeAggregatedMetadata: tc.includeAggregatedMetadata,
			})

			if err != nil {
				t.Fatalf("Error while calling Allocation API %v", err)
			}
			if apiResponse.Code != 200 {
				t.Errorf("API returned non-200 code")
			}

			// Store Allocation Pod Label Results
			for pod, allocationResponseItem := range apiResponse.Data[0] {
				podLabels, ok := podMap[pod]
				if !ok {
					t.Logf("Pod Information Missing from Prometheus %s", pod)
					continue
				}
				podLabels.InAlloc = true
				podLabels.AllocLabels = allocationResponseItem.Properties.Labels
			}

			// Compare Results
			for pod, podLabels := range podMap {
				t.Logf("Pod: %s", pod)

				// Skip pods that were not alive at the query end. They
				// may have been running earlier in the window but
				// /allocation only reports pods with coincident usage
				// metrics, so label comparisons would be noisy.
				if !podLabels.Alive {
					t.Logf("Skipping %s. Pod Dead at query end.", pod)
					continue
				}
				// Skip pods that were not returned by /allocation. A pod
				// can show up in kube_pod_labels but not in /allocation
				// when it was very short lived or lacked CPU/memory
				// usage samples, which is a window-boundary race rather
				// than a label-propagation bug.
				if !podLabels.InAlloc {
					t.Logf("Skipping %s. Pod not present in /allocation response.", pod)
					continue
				}

				// Prometheus Result will have fewer labels.
				// Allocation has oracle and feature related labels
				for promLabel, promLabelValue := range podLabels.PromLabels {
					allocLabelValue, ok := podLabels.AllocLabels[promLabel]
					if !ok {
						t.Errorf("  - [Fail]: Prometheus Label %s not found in Allocation", promLabel)
						continue
					}
					if allocLabelValue != promLabelValue {
						t.Errorf("  - [Fail]: Alloc %s != Prom %s", allocLabelValue, promLabelValue)
					} else {
						t.Logf("  - [Pass]: Label: %s", promLabel)
					}
				}
			}
		})
	}
}
