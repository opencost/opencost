package prom

import (
	"fmt"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/source"
	promsource "github.com/opencost/opencost/modules/prometheus-source/pkg/prom"
	prometheus "github.com/prometheus/client_golang/api"
)

// Prometheus Metric Diagnostic IDs
const (
	CAdvisorDiagnosticMetricID       = "cadvisorMetric"
	CAdvisorLabelDiagnosticMetricID  = "cadvisorLabel"
	KSMDiagnosticMetricID            = "ksmMetric"
	KSMVersionDiagnosticMetricID     = "ksmVersion"
	KubecostDiagnosticMetricID       = "kubecostMetric"
	ScrapeIntervalDiagnosticMetricID = "scrapeInterval"
	CPUThrottlingDiagnosticMetricID  = "cpuThrottling"

	KubecostRecordingRuleCPUUsageID = "kubecostRecordingRuleCPUUsage"
	CAdvisorWorkingSetBytesMetricID = "cadvisorWorkingSetBytesMetric"
	KSMCPUCapacityMetricID          = "ksmCpuCapacityMetric"
	KSMAllocatableCPUCoresMetricID  = "ksmAllocatableCpuCoresMetric"
)

const DocumentationBaseURL = "https://www.opencost.io/docs/"

// OTel-based diagnostic definitions using OTel Collector metric names
var diagnosticDefinitions = map[string]*diagnosticDefinition{
	CAdvisorDiagnosticMetricID: {
		ID:          CAdvisorDiagnosticMetricID,
		QueryFmt:    `absent_over_time(container_cpu_time{%s}[5m] %s)`,
		Label:       "cAdvisor / kubelet metrics available (OTel)",
		Description: "Determine if container CPU metrics from OTel kubeletstats receiver are available during last 5 minutes.",
		DocLink:     fmt.Sprintf("%s#cadvisor-metrics-available", DocumentationBaseURL),
	},
	KSMDiagnosticMetricID: {
		ID:          KSMDiagnosticMetricID,
		QueryFmt:    `absent_over_time(kube_pod_container_resource_requests{resource="memory",unit="byte",%s}[5m] %s)`,
		Label:       "Kube-state-metrics / k8s cluster receiver available (OTel)",
		Description: "Determine if metrics from kube-state-metrics or OTel k8scluster receiver are available during last 5 minutes.",
		DocLink:     fmt.Sprintf("%s#kube-state-metrics-metrics-available", DocumentationBaseURL),
	},
	KubecostDiagnosticMetricID: {
		ID:          KubecostDiagnosticMetricID,
		QueryFmt:    `absent_over_time(node_cpu_hourly_cost{%s}[5m] %s)`,
		Label:       "Kubecost metrics available",
		Description: "Determine if metrics from Kubecost are available during last 5 minutes.",
	},
	CAdvisorLabelDiagnosticMetricID: {
		ID:          CAdvisorLabelDiagnosticMetricID,
		QueryFmt:    `absent_over_time(container_cpu_time{k8s_container_name!="",k8s_pod_name!="",%s}[5m] %s)`,
		Label:       "Expected OTel container labels available",
		Description: "Determine if expected OTel kubelet labels (k8s_container_name, k8s_pod_name) are present during last 5 minutes.",
		DocLink:     fmt.Sprintf("%s#cadvisor-metrics-available", DocumentationBaseURL),
	},
	KSMVersionDiagnosticMetricID: {
		ID:          KSMVersionDiagnosticMetricID,
		QueryFmt:    `absent_over_time(kube_persistentvolume_capacity_bytes{%s}[5m] %s)`,
		Label:       "Expected kube-state-metrics version found",
		Description: "Determine if metric in required kube-state-metrics version are present during last 5 minutes.",
		DocLink:     fmt.Sprintf("%s#expected-kube-state-metrics-version-found", DocumentationBaseURL),
	},
	ScrapeIntervalDiagnosticMetricID: {
		ID:          ScrapeIntervalDiagnosticMetricID,
		QueryFmt:    `absent_over_time(prometheus_target_interval_length_seconds{%s}[5m] %s)`,
		Label:       "Expected Prometheus self-scrape metrics available",
		Description: "Determine if prometheus has its own self-scraped metrics during the last 5 minutes.",
	},
	CPUThrottlingDiagnosticMetricID: {
		ID: CPUThrottlingDiagnosticMetricID,
		QueryFmt: `avg(increase(container_cpu_cfs_throttled_periods_total{container="cost-model",%s}[10m] %s)) by (container_name,pod_name,namespace)
	/ avg(increase(container_cpu_cfs_periods_total{container="cost-model",%s}[10m] %s)) by (container_name,pod_name,namespace) > 0.2`,
		Label:       "Kubecost is not CPU throttled",
		Description: "Kubecost loading slowly? A kubecost component might be CPU throttled",
	},
	KubecostRecordingRuleCPUUsageID: {
		ID:          KubecostRecordingRuleCPUUsageID,
		QueryFmt:    `absent_over_time(kubecost_container_cpu_usage_irate{%s}[5m] %s)`,
		Label:       "Kubecost's CPU usage recording rule is set up",
		Description: "If the 'kubecost_container_cpu_usage_irate' recording rule is not set up, Allocation pipeline build may put pressure on your Prometheus due to the use of a subquery.",
		DocLink:     "https://www.opencost.io/docs/installation/prometheus",
	},
	CAdvisorWorkingSetBytesMetricID: {
		ID:          CAdvisorWorkingSetBytesMetricID,
		QueryFmt:    `absent_over_time(container_memory_working_set{k8s_container_name!="",k8s_container_name!="POD",%s}[5m] %s)`,
		Label:       "OTel container memory working set metrics available",
		Description: "Determine if OTel container memory working set metrics are available during last 5 minutes.",
	},
	KSMCPUCapacityMetricID: {
		ID:          KSMCPUCapacityMetricID,
		QueryFmt:    `absent_over_time(k8s_node_cpu_capacity{%s}[5m] %s)`,
		Label:       "KSM/OTel had CPU capacity during the last 5 minutes",
		Description: "Determine if KSM or OTel k8scluster receiver had CPU capacity during the last 5 minutes.",
	},
	KSMAllocatableCPUCoresMetricID: {
		ID:          KSMAllocatableCPUCoresMetricID,
		QueryFmt:    `absent_over_time(k8s_node_allocatable_cpu{%s}[5m] %s)`,
		Label:       "KSM/OTel had allocatable CPU cores during the last 5 minutes",
		Description: "Determine if KSM or OTel k8scluster receiver had allocatable CPU cores during the last 5 minutes.",
	},
}

// GetPrometheusMetrics returns a list of the state of Prometheus metric used by opencost using the provided client
func GetPrometheusMetrics(client prometheus.Client, config *promsource.OpenCostPrometheusConfig, offset string) PrometheusDiagnostics {
	ctx := promsource.NewNamedContext(client, config, promsource.DiagnosticContextName)

	var result []*PrometheusDiagnostic
	for _, definition := range diagnosticDefinitions {
		pd := definition.NewDiagnostic(config.ClusterFilter, offset)
		err := pd.executePrometheusDiagnosticQuery(ctx)
		if err != nil {
			log.Errorf("error: %s", err.Error())
		}
		result = append(result, pd)
	}

	return result
}

// GetPrometheusMetricsByID returns a list of the state of specific Prometheus metrics by identifier.
func GetPrometheusMetricsByID(ids []string, client prometheus.Client, config *promsource.OpenCostPrometheusConfig, offset string) PrometheusDiagnostics {
	ctx := promsource.NewNamedContext(client, config, promsource.DiagnosticContextName)

	var result []*PrometheusDiagnostic
	for _, id := range ids {
		if definition, ok := diagnosticDefinitions[id]; ok {
			pd := definition.NewDiagnostic(config.ClusterFilter, offset)
			err := pd.executePrometheusDiagnosticQuery(ctx)
			if err != nil {
				log.Errorf("error: %s", err.Error())
			}
			result = append(result, pd)
		} else {
			log.Warnf("Failed to find diagnostic definition for id: %s", id)
		}
	}

	return result
}

// PrometheusDiagnostics is a PrometheusDiagnostic container with helper methods.
type PrometheusDiagnostics []*PrometheusDiagnostic

// HasFailure returns true if any of the diagnostic tests didn't pass.
func (pd PrometheusDiagnostics) HasFailure() bool {
	for _, p := range pd {
		if !p.Passed {
			return true
		}
	}
	return false
}

// diagnosticDefinition is a definition of a diagnostic that can be used to create new
// PrometheusDiagnostic instances using the definition's fields.
type diagnosticDefinition struct {
	ID          string
	QueryFmt    string
	Label       string
	Description string
	DocLink     string
}

// NewDiagnostic creates a new PrometheusDiagnostic instance using the provided definition data.
func (pdd *diagnosticDefinition) NewDiagnostic(filter string, offset string) *PrometheusDiagnostic {
	var query string
	if pdd.ID == CPUThrottlingDiagnosticMetricID {
		query = fmt.Sprintf(pdd.QueryFmt, filter, offset, filter, offset)
	} else {
		query = fmt.Sprintf(pdd.QueryFmt, filter, offset)
	}

	return &PrometheusDiagnostic{
		ID:          pdd.ID,
		Query:       query,
		Label:       pdd.Label,
		Description: pdd.Description,
		DocLink:     pdd.DocLink,
	}
}

// PrometheusDiagnostic holds information about a metric and the query to ensure it is functional
type PrometheusDiagnostic struct {
	ID          string                `json:"id"`
	Query       string                `json:"query"`
	Label       string                `json:"label"`
	Description string                `json:"description"`
	DocLink     string                `json:"docLink"`
	Result      []*source.QueryResult `json:"result"`
	Passed      bool                  `json:"passed"`
}

// executePrometheusDiagnosticQuery executes a PrometheusDiagnostic query using the given context
func (pd *PrometheusDiagnostic) executePrometheusDiagnosticQuery(ctx *promsource.Context) error {
	resultCh := ctx.Query(pd.Query)
	result, err := resultCh.Await()
	if err != nil {
		return fmt.Errorf("prometheus diagnostic %s failed with error: %s", pd.ID, err)
	}
	if result == nil {
		result = []*source.QueryResult{}
	}
	pd.Result = result
	pd.Passed = len(result) == 0
	return nil
}

func (pd *PrometheusDiagnostic) AsMap() map[string]any {
	return map[string]any{
		"query":   pd.Query,
		"label":   pd.Label,
		"docLink": pd.DocLink,
		"result":  pd.Result,
		"passed":  pd.Passed,
	}
}
