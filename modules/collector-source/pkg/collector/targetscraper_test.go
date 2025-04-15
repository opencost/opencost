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

const opencostScrape = `
# HELP kubecost_cluster_management_cost kubecost_cluster_management_cost Hourly cost paid as a cluster management fee.
# TYPE kubecost_cluster_management_cost gauge
kubecost_cluster_management_cost{provisioner_name="GKE"} 0.1
# HELP kubecost_network_zone_egress_cost kubecost_network_zone_egress_cost Total cost per GB egress across zones
# TYPE kubecost_network_zone_egress_cost gauge
kubecost_network_zone_egress_cost 0.01
# HELP kubecost_network_region_egress_cost kubecost_network_region_egress_cost Total cost per GB egress across regions
# TYPE kubecost_network_region_egress_cost gauge
kubecost_network_region_egress_cost 0.01
# HELP kubecost_network_internet_egress_cost kubecost_network_internet_egress_cost Total cost per GB of internet egress.
# TYPE kubecost_network_internet_egress_cost gauge
kubecost_network_internet_egress_cost 0.12
# HELP pv_hourly_cost pv_hourly_cost Cost per GB per hour on a persistent disk
# TYPE pv_hourly_cost gauge
pv_hourly_cost{persistentvolume="pvc-1",provider_id="pvc-1",volumename="pvc-1"} 5.479452054794521e-05
pv_hourly_cost{persistentvolume="pvc-2",provider_id="pvc-2",volumename="pvc-2"} 5.479452054794521e-05
# HELP kubecost_load_balancer_cost kubecost_load_balancer_cost Hourly cost of load balancer
# TYPE kubecost_load_balancer_cost gauge
kubecost_load_balancer_cost{ingress_ip="127.0.0.1",namespace="namespace1",service_name="service1"} 0.025
# HELP container_cpu_allocation container_cpu_allocation Percent of a single CPU used in a minute
# TYPE container_cpu_allocation gauge
# HELP node_total_hourly_cost node_total_hourly_cost Total node cost per hour
# TYPE node_total_hourly_cost gauge
node_total_hourly_cost{arch="amd64",instance="node1",instance_type="e2-standard-2",node="node1",provider_id="node1",region="region1"} 0.06631302438846588
node_total_hourly_cost{arch="amd64",instance="node2",instance_type="e2-standard-2",node="node2",provider_id="node2",region="region1"} 0.06631302438846588
# HELP node_cpu_hourly_cost node_cpu_hourly_cost hourly cost for each cpu on this node
# TYPE node_cpu_hourly_cost gauge
node_cpu_hourly_cost{arch="amd64",instance="node1",instance_type="e2-standard-2",node="node1",provider_id="node1",region="region1"} 0.021811590000000002
node_cpu_hourly_cost{arch="amd64",instance="node2",instance_type="e2-standard-2",node="node2",provider_id="node2",region="region1"} 0.021811590000000002
# HELP node_ram_hourly_cost node_ram_hourly_cost hourly cost for each gb of ram on this node
# TYPE node_ram_hourly_cost gauge
node_ram_hourly_cost{arch="amd64",instance="node1",instance_type="e2-standard-2",node="node1",provider_id="node1",region="region1"} 0.00292353
node_ram_hourly_cost{arch="amd64",instance="node2",instance_type="e2-standard-2",node="node2",provider_id="node2",region="region1"} 0.00292353
# HELP node_gpu_hourly_cost node_gpu_hourly_cost hourly cost for each gpu on this node
# TYPE node_gpu_hourly_cost gauge
node_gpu_hourly_cost{arch="amd64",instance="node1",instance_type="e2-standard-2",node="node1",provider_id="node1",region="region1"} 0
node_gpu_hourly_cost{arch="amd64",instance="node2",instance_type="e2-standard-2",node="node2",provider_id="node2",region="region1"} 0
# HELP node_gpu_count node_gpu_count count of gpu on this node
# TYPE node_gpu_count gauge
node_gpu_count{arch="amd64",instance="node1",instance_type="e2-standard-2",node="node1",provider_id="node1",region="region1"} 0
node_gpu_count{arch="amd64",instance="node2",instance_type="e2-standard-2",node="node2",provider_id="node2",region="region1"} 0
# HELP kubecost_node_is_spot kubecost_node_is_spot Cloud provider info about node preemptibility
# TYPE kubecost_node_is_spot gauge
kubecost_node_is_spot{arch="amd64",instance="node1",instance_type="e2-standard-2",node="node1",provider_id="node1",region="region1"} 0
kubecost_node_is_spot{arch="amd64",instance="node2",instance_type="e2-standard-2",node="node2",provider_id="node2",region="region1"} 0
# HELP ignore_fake_metric fake metric that the scrapper should ignore
# TYPE ignore_fake_metric gauge
ignore_fake_metric{container="container1",instance="node1",namespace="namespace1",node="node1",pod="pod1"} 0.02
# HELP container_cpu_allocation container_cpu_allocation Percent of a single CPU used in a minute
# TYPE container_cpu_allocation gauge
container_cpu_allocation{container="container1",instance="node1",namespace="namespace1",node="node1",pod="pod1"} 0.02
container_cpu_allocation{container="container2",instance="node2",namespace="namespace1",node="node2",pod="pod2"} 0.01
# HELP container_memory_allocation_bytes container_memory_allocation_bytes Bytes of RAM used
# TYPE container_memory_allocation_bytes gauge
container_memory_allocation_bytes{container="container1",instance="node1",namespace="namespace1",node="node1",pod="pod1"} 1.1528192e+07
container_memory_allocation_bytes{container="container2",instance="node2",namespace="namespace1",node="node2",pod="pod2"} 1e+07
# HELP container_gpu_allocation container_gpu_allocation GPU used
# TYPE container_gpu_allocation gauge
container_gpu_allocation{container="container1",instance="node1",namespace="namespace1",node="node1",pod="pod1"} 0
container_gpu_allocation{container="container2",instance="node2",namespace="namespace1",node="node2",pod="pod2"} 0
# HELP pod_pvc_allocation pod_pvc_allocation Bytes used by a PVC attached to a pod
# TYPE pod_pvc_allocation gauge
pod_pvc_allocation{namespace="namespace1",persistentvolume="pvc-1",persistentvolumeclaim="pvc1",pod="pod1"} 3.4359738368e+10
pod_pvc_allocation{namespace="namespace1",persistentvolume="pvc-2",persistentvolumeclaim="pvc2",pod="pod2"} 3.4359738368e+10
`

func TestTargetScraper_Scrape(t *testing.T) {

	tests := []struct {
		name            string
		scrapperFactory func(collector MetricsCollector) *TargetScraper
		expected        []UpdateArgs
	}{
		{
			name: "Network Scrape",
			scrapperFactory: func(collector MetricsCollector) *TargetScraper {
				return NewTargetScrapper(
					NewMockTargetProvider(target.NewStringTarget(networkScape)),
					collector,
					nil,
					false,
				)
			},
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
		{
			name: "Opencost Metric",
			scrapperFactory: func(collector MetricsCollector) *TargetScraper {
				return NewOpencostTargetScraper(NewMockTargetProvider(target.NewStringTarget(opencostScrape)),
					collector,
				)
			},
			expected: []UpdateArgs{
				{
					metricName: KubecostClusterManagementCost,
					labels: map[string]string{
						"provisioner_name": "GKE",
					},
					value: 0.1,
				},
				{
					metricName: KubecostNetworkZoneEgressCost,
					labels:     map[string]string{},
					value:      0.01,
				},
				{
					metricName: KubecostNetworkRegionEgressCost,
					labels:     map[string]string{},
					value:      0.01,
				},
				{
					metricName: KubecostNetworkInternetEgressCost,
					labels:     map[string]string{},
					value:      0.12,
				},
				{
					metricName: PVHourlyCost,
					labels: map[string]string{
						"persistentvolume": "pvc-1",
						"provider_id":      "pvc-1",
						"volumename":       "pvc-1",
					},
					value: 5.479452054794521e-05,
				},
				{
					metricName: PVHourlyCost,
					labels: map[string]string{
						"persistentvolume": "pvc-2",
						"provider_id":      "pvc-2",
						"volumename":       "pvc-2",
					},
					value: 5.479452054794521e-05,
				},
				{
					metricName: KubecostLoadBalancerCost,
					labels: map[string]string{
						"ingress_ip":   "127.0.0.1",
						"namespace":    "namespace1",
						"service_name": "service1",
					},
					value: 0.025,
				},
				{
					metricName: NodeTotalHourlyCost,
					labels: map[string]string{
						"arch":          "amd64",
						"instance":      "node1",
						"instance_type": "e2-standard-2",
						"node":          "node1",
						"provider_id":   "node1",
						"region":        "region1",
					},
					value: 0.06631302438846588,
				},
				{
					metricName: NodeTotalHourlyCost,
					labels: map[string]string{
						"arch":          "amd64",
						"instance":      "node2",
						"instance_type": "e2-standard-2",
						"node":          "node2",
						"provider_id":   "node2",
						"region":        "region1",
					},
					value: 0.06631302438846588,
				},
				{
					metricName: NodeCPUHourlyCost,
					labels: map[string]string{
						"arch":          "amd64",
						"instance":      "node1",
						"instance_type": "e2-standard-2",
						"node":          "node1",
						"provider_id":   "node1",
						"region":        "region1",
					},
					value: 0.021811590000000002,
				},
				{
					metricName: NodeCPUHourlyCost,
					labels: map[string]string{
						"arch":          "amd64",
						"instance":      "node2",
						"instance_type": "e2-standard-2",
						"node":          "node2",
						"provider_id":   "node2",
						"region":        "region1",
					},
					value: 0.021811590000000002,
				},
				{
					metricName: NodeRAMHourlyCost,
					labels: map[string]string{
						"arch":          "amd64",
						"instance":      "node1",
						"instance_type": "e2-standard-2",
						"node":          "node1",
						"provider_id":   "node1",
						"region":        "region1",
					},
					value: 0.00292353,
				},
				{
					metricName: NodeRAMHourlyCost,
					labels: map[string]string{
						"arch":          "amd64",
						"instance":      "node2",
						"instance_type": "e2-standard-2",
						"node":          "node2",
						"provider_id":   "node2",
						"region":        "region1",
					},
					value: 0.00292353,
				},
				{
					metricName: NodeGPUHourlyCost,
					labels: map[string]string{
						"arch":          "amd64",
						"instance":      "node1",
						"instance_type": "e2-standard-2",
						"node":          "node1",
						"provider_id":   "node1",
						"region":        "region1",
					},
					value: 0,
				},
				{
					metricName: NodeGPUHourlyCost,
					labels: map[string]string{
						"arch":          "amd64",
						"instance":      "node2",
						"instance_type": "e2-standard-2",
						"node":          "node2",
						"provider_id":   "node2",
						"region":        "region1",
					},
					value: 0,
				},
				{
					metricName: NodeGPUCount,
					labels: map[string]string{
						"arch":          "amd64",
						"instance":      "node1",
						"instance_type": "e2-standard-2",
						"node":          "node1",
						"provider_id":   "node1",
						"region":        "region1",
					},
					value: 0,
				},
				{
					metricName: NodeGPUCount,
					labels: map[string]string{
						"arch":          "amd64",
						"instance":      "node2",
						"instance_type": "e2-standard-2",
						"node":          "node2",
						"provider_id":   "node2",
						"region":        "region1",
					},
					value: 0,
				},
				{
					metricName: KubecostNodeIsSpot,
					labels: map[string]string{
						"arch":          "amd64",
						"instance":      "node1",
						"instance_type": "e2-standard-2",
						"node":          "node1",
						"provider_id":   "node1",
						"region":        "region1",
					},
					value: 0,
				},
				{
					metricName: KubecostNodeIsSpot,
					labels: map[string]string{
						"arch":          "amd64",
						"instance":      "node2",
						"instance_type": "e2-standard-2",
						"node":          "node2",
						"provider_id":   "node2",
						"region":        "region1",
					},
					value: 0,
				},
				{
					metricName: ContainerCPUAllocation,
					labels: map[string]string{
						"container": "container1",
						"instance":  "node1",
						"namespace": "namespace1",
						"node":      "node1",
						"pod":       "pod1",
					},
					value: 0.02,
				},
				{
					metricName: ContainerCPUAllocation,
					labels: map[string]string{
						"container": "container2",
						"instance":  "node2",
						"namespace": "namespace1",
						"node":      "node2",
						"pod":       "pod2",
					},
					value: 0.01,
				},
				{
					metricName: ContainerMemoryAllocationBytes,
					labels: map[string]string{
						"container": "container1",
						"instance":  "node1",
						"namespace": "namespace1",
						"node":      "node1",
						"pod":       "pod1",
					},
					value: 1.1528192e+07,
				},
				{
					metricName: ContainerMemoryAllocationBytes,
					labels: map[string]string{
						"container": "container2",
						"instance":  "node2",
						"namespace": "namespace1",
						"node":      "node2",
						"pod":       "pod2",
					},
					value: 1e+07,
				},
				{
					metricName: ContainerGPUAllocation,
					labels: map[string]string{
						"container": "container1",
						"instance":  "node1",
						"namespace": "namespace1",
						"node":      "node1",
						"pod":       "pod1",
					},
					value: 0,
				},
				{
					metricName: ContainerGPUAllocation,
					labels: map[string]string{
						"container": "container2",
						"instance":  "node2",
						"namespace": "namespace1",
						"node":      "node2",
						"pod":       "pod2",
					},
					value: 0,
				},
				{
					metricName: PodPVCAllocation,
					labels: map[string]string{
						"namespace":             "namespace1",
						"persistentvolume":      "pvc-1",
						"persistentvolumeclaim": "pvc1",
						"pod":                   "pod1",
					},
					value: 3.4359738368e+10,
				},
				{
					metricName: PodPVCAllocation,
					labels: map[string]string{
						"namespace":             "namespace1",
						"persistentvolume":      "pvc-2",
						"persistentvolumeclaim": "pvc2",
						"pod":                   "pod2",
					},
					value: 3.4359738368e+10,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			updateRecorder := UpdateRecorderCollector{}
			scrapper := tt.scrapperFactory(&updateRecorder)
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
