package prom

import (
	"reflect"
	"strings"
	"testing"
)

func TestDiagnosticDefinition_NewDiagnostic(t *testing.T) {
	tests := []struct {
		id    string
		query string
	}{
		{
			id:    CAdvisorDiagnosticMetricID,
			query: `absent_over_time(container_cpu_usage_seconds_total{FILTER}[5m] OFFSET)`,
		},
		{
			id:    KSMDiagnosticMetricID,
			query: `absent_over_time(kube_pod_container_resource_requests{resource="memory", unit="byte", FILTER}[5m] OFFSET)`,
		},
		{
			id:    KubecostDiagnosticMetricID,
			query: `absent_over_time(node_cpu_hourly_cost{FILTER}[5m] OFFSET)`,
		},
		{
			id:    CAdvisorLabelDiagnosticMetricID,
			query: `absent_over_time(container_cpu_usage_seconds_total{container!="",pod!="", FILTER}[5m] OFFSET)`,
		},
		{
			id:    KSMVersionDiagnosticMetricID,
			query: `absent_over_time(kube_persistentvolume_capacity_bytes{FILTER}[5m] OFFSET)`,
		},
		{
			id:    ScrapeIntervalDiagnosticMetricID,
			query: `absent_over_time(prometheus_target_interval_length_seconds{FILTER}[5m]  OFFSET)`,
		},
		{
			id: CPUThrottlingDiagnosticMetricID,
			query: `avg(increase(container_cpu_cfs_throttled_periods_total{container="cost-model", FILTER}[10m] OFFSET)) by (container_name, pod_name, namespace)
	/ avg(increase(container_cpu_cfs_periods_total{container="cost-model",FILTER}[10m] OFFSET)) by (container_name, pod_name, namespace) > 0.2`,
		},
		{
			id:    KubecostRecordingRuleCPUUsageID,
			query: `absent_over_time(kubecost_container_cpu_usage_irate{FILTER}[5m] OFFSET)`,
		},
		{
			id:    CAdvisorWorkingSetBytesMetricID,
			query: `absent_over_time(container_memory_working_set_bytes{container="cost-model", container!="POD", instance!="", FILTER}[5m] OFFSET)`,
		},
		{
			id:    KSMCPUCapacityMetricID,
			query: `absent_over_time(kube_node_status_capacity_cpu_cores{FILTER}[5m] OFFSET)`,
		},
		{
			id:    KSMAllocatableCPUCoresMetricID,
			query: `absent_over_time(kube_node_status_allocatable_cpu_cores{FILTER}[5m] OFFSET)`,
		},
	}

	inputs := []struct {
		name   string
		filter string
		offset string
	}{
		{name: "empty"},
		{name: "filter", filter: `cluster_id="dev"`},
		{name: "offset", offset: "offset 5m"},
		{name: "filter and offset", filter: `cluster_id="dev"`, offset: "offset 5m"},
		{name: "literal percent", filter: `cluster_id=~"dev.*",team="50%"`, offset: "offset 1h"},
	}

	for _, tt := range tests {
		t.Run(tt.id, func(t *testing.T) {
			definition, ok := diagnosticDefinitions[tt.id]
			if !ok {
				t.Fatalf("missing diagnostic definition %q", tt.id)
			}
			for _, input := range inputs {
				t.Run(input.name, func(t *testing.T) {
					query := strings.NewReplacer("FILTER", input.filter, "OFFSET", input.offset).Replace(tt.query)
					want := &PrometheusDiagnostic{
						ID:          tt.id,
						Query:       query,
						Label:       definition.Label,
						Description: definition.Description,
						DocLink:     definition.DocLink,
					}
					got := definition.NewDiagnostic(input.filter, input.offset)
					if !reflect.DeepEqual(got, want) {
						t.Errorf("NewDiagnostic() = %#v, want %#v", got, want)
					}
				})
			}
		})
	}
}
