package collector

import (
	"testing"

	"github.com/opencost/opencost/modules/collector-source/pkg/metrics/target"
)

const networkScape = `
# HELP kubecost_pod_network_egress_bytes kubecost_pod_network_egress_bytes_total egressed byte counts by pod.
# TYPE kubecost_pod_network_egress_bytes counter
kubecost_pod_network_egress_bytes_total{pod_name="pod1",namespace="namespace1",internet="false",same_region="true",same_zone="true",service="service1"} 3127969647
kubecost_pod_network_egress_bytes_total{pod_name="pod2",namespace="namespace1",internet="true",same_region="false",same_zone="false",service=""} 335188219
# HELP kubecost_pod_network_ingress_bytes kubecost_pod_network_ingress_bytes_total ingressed byte counts by pod.
# TYPE kubecost_pod_network_ingress_bytes counter
kubecost_pod_network_ingress_bytes_total{pod_name="pod1",namespace="namespace1",internet="true",same_region="false",same_zone="false",service="service1"} 17941460
kubecost_pod_network_ingress_bytes_total{pod_name="pod2",namespace="namespace1",internet="false",same_region="true",same_zone="false",service=""} 13948766
# HELP kubecost_network_costs_parsed_entries kubecost_network_costs_parsed_entries total parsed conntrack entries.
# TYPE kubecost_network_costs_parsed_entries gauge
# HELP kubecost_network_costs_parse_time kubecost_network_costs_parse_time total time in milliseconds it took to parse conntrack entries.
# TYPE kubecost_network_costs_parse_time gauge
# EOF
`

func TestTargetScraper_Scrape(t *testing.T) {
	tests := []struct {
		name     string
		target   target.ScrapeTarget
		expected []UpdateArgs
	}{
		{
			name:   "Network Scrape",
			target: target.NewStringTarget(networkScape),
			expected: []UpdateArgs{
				{
					metricName: KubecostPodNetworkEgressBytesTotal,
					labels: map[string]string{
						"pod_name":    "pod1",
						"namespace":   "namespace1",
						"internet":    "false",
						"same_region": "true",
						"same_zone":   "true",
						"service":     "service1",
					},
					value:                 3127969647,
					timestamp:             nil,
					additionalInformation: nil,
				},
				{
					metricName: KubecostPodNetworkEgressBytesTotal,
					labels: map[string]string{
						"pod_name":    "pod2",
						"namespace":   "namespace1",
						"internet":    "true",
						"same_region": "false",
						"same_zone":   "false",
						"service":     "",
					},
					value:                 335188219,
					timestamp:             nil,
					additionalInformation: nil,
				},
				{
					metricName: "kubecost_pod_network_ingress_bytes_total",
					labels: map[string]string{
						"pod_name":    "pod1",
						"namespace":   "namespace1",
						"internet":    "true",
						"same_region": "false",
						"same_zone":   "false",
						"service":     "service1",
					},
					value:                 17941460,
					timestamp:             nil,
					additionalInformation: nil,
				},
				{
					metricName: "kubecost_pod_network_ingress_bytes_total",
					labels: map[string]string{
						"pod_name":    "pod2",
						"namespace":   "namespace1",
						"internet":    "false",
						"same_region": "true",
						"same_zone":   "false",
						"service":     "",
					},
					value:                 13948766,
					timestamp:             nil,
					additionalInformation: nil,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			updateRecorder := UpdateRecorderCollector{}
			scrapper := &TargetScraper{
				targetProvider: NewMockTargetProvider(tt.target),
				collector:      &updateRecorder,
			}
			scrapper.Scrape()

			if len(updateRecorder.updateArgs) != len(tt.expected) {
				t.Errorf("Expected result length of %d, got %d", len(tt.expected), len(updateRecorder.updateArgs))
			}

			for i, expected := range tt.expected {
				updateArg := updateRecorder.updateArgs[i]
				err := expected.equals(updateArg)
				if err != nil {
					t.Errorf("Result did not match expected at index %d: %s", i, err.Error())
				}
			}
		})
	}
}
