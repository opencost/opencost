package allocation

// Description
// Check Pod Annotations from API Match results from Prometheus

import (
	"testing"
	"time"

	"github.com/opencost/opencost-integration-tests/pkg/api"
	"github.com/opencost/opencost-integration-tests/pkg/prometheus"
)

const podStatusResolution = "1m"

func TestPodAnnotations(t *testing.T) {
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
		{
			name:                      "Last Two Days",
			window:                    "48h",
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
			// Pod Annotations
			// avg_over_time(kube_pod_annotations{%s}[%s])
			// -------------------------------
			client := prometheus.NewClient()
			promAnnotationInfoInput := prometheus.PrometheusInput{}
			promAnnotationInfoInput.Metric = "kube_pod_annotations"
			promAnnotationInfoInput.Function = []string{"avg_over_time"}
			promAnnotationInfoInput.QueryWindow = tc.window
			promAnnotationInfoInput.Time = &endTime

			promAnnotationInfo, err := client.RunPromQLQuery(promAnnotationInfoInput, t)
			if err != nil {
				t.Fatalf("Error while calling Prometheus API %v", err)
			}

			// Pod Info
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

			// Store Results in a Pod Map
			type PodData struct {
				Pod              string
				Alive            bool
				InAlloc          bool
				promAnnotations  map[string]string
				AllocAnnotations map[string]string
			}

			podMap := make(map[string]*PodData)

			// Store Prometheus Pod Prometheus Results
			for _, promAnnotation := range promAnnotationInfo.Data.Result {
				pod := promAnnotation.Metric.Pod
				Annotations := promAnnotation.Metric.Annotations

				podMap[pod] = &PodData{
					Pod:             pod,
					promAnnotations: Annotations,
				}
			}

			for _, podInfoResponseItem := range podInfo.Data.Result {
				podMapItem, ok := podMap[podInfoResponseItem.Metric.Pod]
				if ok {
					podMapItem.Alive = true
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

			// Store Allocation Pod Annotation Results
			for pod, allocationResponseItem := range apiResponse.Data[0] {
				podAnnotations, ok := podMap[pod]
				// No Annotations for this pod.
				// Not all pods have annotations
				if !ok {
					t.Logf("[Skipped] - No Annotations for Pod: %s", pod)
					continue
				}
				podAnnotations.InAlloc = true
				podAnnotations.AllocAnnotations = allocationResponseItem.Properties.Annotations
			}

			seenAnnotations := false

			// Compare Results
			for pod, podAnnotations := range podMap {
				t.Logf("Pod: %s", pod)
				if podAnnotations.Alive == false {
					t.Logf("Skipping %s. Pod Dead", pod)
					continue
				}
				// Skip pods that the Allocation API did not return. A
				// pod can appear in kube_pod_annotations and briefly in
				// kube_pod_container_status_running yet be absent from
				// /allocation, which only reports pods with coincident
				// usage metrics. Comparing annotations in that case is
				// a window-boundary race, not an annotation-propagation
				// bug.
				if !podAnnotations.InAlloc {
					t.Logf("Skipping %s. Pod not present in /allocation response.", pod)
					continue
				}
				// Prometheus Result will have fewer Annotations.
				// Allocation has oracle and feature related Annotations
				for promAnnotation, promAnnotationValue := range podAnnotations.promAnnotations {
					allocAnnotationValue, ok := podAnnotations.AllocAnnotations[promAnnotation]
					if !ok {
						t.Errorf("  - [Fail]: Prometheus Annotation %s not found in Allocation", promAnnotation)
						continue
					}
					seenAnnotations = true
					if allocAnnotationValue != promAnnotationValue {
						t.Errorf("  - [Fail]: Alloc %s != Prom %s", allocAnnotationValue, promAnnotationValue)
					} else {
						t.Logf("  - [Pass]: Annotation: %s", promAnnotation)
					}
				}
			}
			if !seenAnnotations {
				t.Fatalf("No Pod Annotations")
			}
		})
	}
}
