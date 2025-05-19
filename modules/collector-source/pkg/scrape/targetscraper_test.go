package scrape

import (
	"reflect"
	"testing"

	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
	"github.com/opencost/opencost/modules/collector-source/pkg/scrape/target"
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

const dcgmScrape = `
# HELP DCGM_FI_PROF_GR_ENGINE_ACTIVE Ratio of time the graphics engine is active.
# TYPE DCGM_FI_PROF_GR_ENGINE_ACTIVE gauge
DCGM_FI_PROF_GR_ENGINE_ACTIVE{gpu="0",UUID="GPU-1",pci_bus_id="00000000:00:0A.0",device="nvidia0",modelName="Tesla T4",Hostname="localhost"} 0.999999
# HELP DCGM_FI_DEV_DEC_UTIL Decoder utilization (in %).
# TYPE DCGM_FI_DEV_DEC_UTIL gauge
DCGM_FI_DEV_DEC_UTIL{gpu="0",UUID="GPU-1",pci_bus_id="00000000:00:0A.0",device="nvidia0",modelName="Tesla T4",Hostname="localhost"} 0 
`

func TestTargetScraper_Scrape(t *testing.T) {
	tests := []struct {
		name                 string
		scrapeText           string
		targetScraperFactory func(provider target.TargetProvider) *TargetScraper
		expected             []metric.Update
	}{
		{
			name:                 "Network Scrape",
			scrapeText:           networkScape,
			targetScraperFactory: newNetworkTargetScraper,
			expected: []metric.Update{
				{
					Name: KubecostPodNetworkEgressBytesTotal,
					Labels: map[string]string{
						"pod_name":    "pod1",
						"namespace":   "namespace1",
						"internet":    "false",
						"same_region": "true",
						"same_zone":   "true",
						"service":     "service1",
					},
					Value: 3127969647,
				},
				{
					Name: KubecostPodNetworkEgressBytesTotal,
					Labels: map[string]string{
						"pod_name":    "pod2",
						"namespace":   "namespace1",
						"internet":    "true",
						"same_region": "false",
						"same_zone":   "false",
						"service":     "",
					},
					Value: 335188219,
				},
				{
					Name: KubecostPodNetworkIngressBytesTotal,
					Labels: map[string]string{
						"pod_name":    "pod1",
						"namespace":   "namespace1",
						"internet":    "true",
						"same_region": "false",
						"same_zone":   "false",
						"service":     "service1",
					},
					Value: 17941460,
				},
				{
					Name: KubecostPodNetworkIngressBytesTotal,
					Labels: map[string]string{
						"pod_name":    "pod2",
						"namespace":   "namespace1",
						"internet":    "false",
						"same_region": "true",
						"same_zone":   "false",
						"service":     "",
					},
					Value: 13948766,
				},
			},
		},
		{
			name:                 "Opencost Metric",
			scrapeText:           opencostScrape,
			targetScraperFactory: newOpencostTargetScraper,
			expected: []metric.Update{
				{
					Name: KubecostClusterManagementCost,
					Labels: map[string]string{
						"provisioner_name": "GKE",
					},
					Value: 0.1,
				},
				{
					Name:  KubecostNetworkZoneEgressCost,
					Value: 0.01,
				},
				{
					Name:  KubecostNetworkRegionEgressCost,
					Value: 0.01,
				},
				{
					Name:  KubecostNetworkInternetEgressCost,
					Value: 0.12,
				},
				{
					Name: PVHourlyCost,
					Labels: map[string]string{
						"persistentvolume": "pvc-1",
						"provider_id":      "pvc-1",
						"volumename":       "pvc-1",
					},
					Value: 5.479452054794521e-05,
				},
				{
					Name: PVHourlyCost,
					Labels: map[string]string{
						"persistentvolume": "pvc-2",
						"provider_id":      "pvc-2",
						"volumename":       "pvc-2",
					},
					Value: 5.479452054794521e-05,
				},
				{
					Name: KubecostLoadBalancerCost,
					Labels: map[string]string{
						"ingress_ip":   "127.0.0.1",
						"namespace":    "namespace1",
						"service_name": "service1",
					},
					Value: 0.025,
				},
				{
					Name: NodeTotalHourlyCost,
					Labels: map[string]string{
						"arch":          "amd64",
						"instance":      "node1",
						"instance_type": "e2-standard-2",
						"node":          "node1",
						"provider_id":   "node1",
						"region":        "region1",
					},
					Value: 0.06631302438846588,
				},
				{
					Name: NodeTotalHourlyCost,
					Labels: map[string]string{
						"arch":          "amd64",
						"instance":      "node2",
						"instance_type": "e2-standard-2",
						"node":          "node2",
						"provider_id":   "node2",
						"region":        "region1",
					},
					Value: 0.06631302438846588,
				},
				{
					Name: NodeCPUHourlyCost,
					Labels: map[string]string{
						"arch":          "amd64",
						"instance":      "node1",
						"instance_type": "e2-standard-2",
						"node":          "node1",
						"provider_id":   "node1",
						"region":        "region1",
					},
					Value: 0.021811590000000002,
				},
				{
					Name: NodeCPUHourlyCost,
					Labels: map[string]string{
						"arch":          "amd64",
						"instance":      "node2",
						"instance_type": "e2-standard-2",
						"node":          "node2",
						"provider_id":   "node2",
						"region":        "region1",
					},
					Value: 0.021811590000000002,
				},
				{
					Name: NodeRAMHourlyCost,
					Labels: map[string]string{
						"arch":          "amd64",
						"instance":      "node1",
						"instance_type": "e2-standard-2",
						"node":          "node1",
						"provider_id":   "node1",
						"region":        "region1",
					},
					Value: 0.00292353,
				},
				{
					Name: NodeRAMHourlyCost,
					Labels: map[string]string{
						"arch":          "amd64",
						"instance":      "node2",
						"instance_type": "e2-standard-2",
						"node":          "node2",
						"provider_id":   "node2",
						"region":        "region1",
					},
					Value: 0.00292353,
				},
				{
					Name: NodeGPUHourlyCost,
					Labels: map[string]string{
						"arch":          "amd64",
						"instance":      "node1",
						"instance_type": "e2-standard-2",
						"node":          "node1",
						"provider_id":   "node1",
						"region":        "region1",
					},
					Value: 0,
				},
				{
					Name: NodeGPUHourlyCost,
					Labels: map[string]string{
						"arch":          "amd64",
						"instance":      "node2",
						"instance_type": "e2-standard-2",
						"node":          "node2",
						"provider_id":   "node2",
						"region":        "region1",
					},
					Value: 0,
				},
				{
					Name: NodeGPUCount,
					Labels: map[string]string{
						"arch":          "amd64",
						"instance":      "node1",
						"instance_type": "e2-standard-2",
						"node":          "node1",
						"provider_id":   "node1",
						"region":        "region1",
					},
					Value: 0,
				},
				{
					Name: NodeGPUCount,
					Labels: map[string]string{
						"arch":          "amd64",
						"instance":      "node2",
						"instance_type": "e2-standard-2",
						"node":          "node2",
						"provider_id":   "node2",
						"region":        "region1",
					},
					Value: 0,
				},
				{
					Name: KubecostNodeIsSpot,
					Labels: map[string]string{
						"arch":          "amd64",
						"instance":      "node1",
						"instance_type": "e2-standard-2",
						"node":          "node1",
						"provider_id":   "node1",
						"region":        "region1",
					},
					Value: 0,
				},
				{
					Name: KubecostNodeIsSpot,
					Labels: map[string]string{
						"arch":          "amd64",
						"instance":      "node2",
						"instance_type": "e2-standard-2",
						"node":          "node2",
						"provider_id":   "node2",
						"region":        "region1",
					},
					Value: 0,
				},
				{
					Name: ContainerCPUAllocation,
					Labels: map[string]string{
						"container": "container1",
						"instance":  "node1",
						"namespace": "namespace1",
						"node":      "node1",
						"pod":       "pod1",
					},
					Value: 0.02,
				},
				{
					Name: ContainerCPUAllocation,
					Labels: map[string]string{
						"container": "container2",
						"instance":  "node2",
						"namespace": "namespace1",
						"node":      "node2",
						"pod":       "pod2",
					},
					Value: 0.01,
				},
				{
					Name: ContainerMemoryAllocationBytes,
					Labels: map[string]string{
						"container": "container1",
						"instance":  "node1",
						"namespace": "namespace1",
						"node":      "node1",
						"pod":       "pod1",
					},
					Value: 1.1528192e+07,
				},
				{
					Name: ContainerMemoryAllocationBytes,
					Labels: map[string]string{
						"container": "container2",
						"instance":  "node2",
						"namespace": "namespace1",
						"node":      "node2",
						"pod":       "pod2",
					},
					Value: 1e+07,
				},
				{
					Name: ContainerGPUAllocation,
					Labels: map[string]string{
						"container": "container1",
						"instance":  "node1",
						"namespace": "namespace1",
						"node":      "node1",
						"pod":       "pod1",
					},
					Value: 0,
				},
				{
					Name: ContainerGPUAllocation,
					Labels: map[string]string{
						"container": "container2",
						"instance":  "node2",
						"namespace": "namespace1",
						"node":      "node2",
						"pod":       "pod2",
					},
					Value: 0,
				},
				{
					Name: PodPVCAllocation,
					Labels: map[string]string{
						"namespace":             "namespace1",
						"persistentvolume":      "pvc-1",
						"persistentvolumeclaim": "pvc1",
						"pod":                   "pod1",
					},
					Value: 3.4359738368e+10,
				},
				{
					Name: PodPVCAllocation,
					Labels: map[string]string{
						"namespace":             "namespace1",
						"persistentvolume":      "pvc-2",
						"persistentvolumeclaim": "pvc2",
						"pod":                   "pod2",
					},
					Value: 3.4359738368e+10,
				},
			},
		},
		{
			name:                 "GPU Metric",
			scrapeText:           dcgmScrape,
			targetScraperFactory: newDCGMTargetScraper,
			expected: []metric.Update{
				{
					Name: DCGMFIPROFGRENGINEACTIVE,
					Labels: map[string]string{
						"gpu":        "0",
						"UUID":       "GPU-1",
						"pci_bus_id": "00000000:00:0A.0",
						"device":     "nvidia0",
						"modelName":  "Tesla T4",
						"Hostname":   "localhost",
					},
					Value: 0.999999,
				},
				{
					Name: DCGMFIDEVDECUTIL,
					Labels: map[string]string{
						"gpu":        "0",
						"UUID":       "GPU-1",
						"pci_bus_id": "00000000:00:0A.0",
						"device":     "nvidia0",
						"modelName":  "Tesla T4",
						"Hostname":   "localhost",
					},
					Value: 0,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scraper := tt.targetScraperFactory(target.NewDefaultTargetProvider(target.NewStringTarget(tt.scrapeText)))
			scrapeResults := scraper.Scrape()

			if len(scrapeResults) != len(tt.expected) {
				t.Errorf("Expected result length of %d, got %d", len(tt.expected), len(scrapeResults))
			}

			for i, expected := range tt.expected {
				got := scrapeResults[i]
				if !reflect.DeepEqual(expected, got) {
					t.Errorf("Result did not match expected at index %d: got %v, want %v", i, got, expected)
				}
			}
		})
	}
}
