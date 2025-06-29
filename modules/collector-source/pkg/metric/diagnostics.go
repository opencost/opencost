package metric

import (
	"fmt"
	"maps"
	"sync"
)

// Collector Metric Diagnostic IDs
const (
	// KubecostDiagnosticMetricID is the identifier for the metric used to determine if Kubecost metrics are being scraped.
	KubecostDiagnosticMetricID = "kubecostMetric"

	// NodesDiagnosticMetricID is the identifier for the query used to determine if the node CPU cores capacity is being scraped
	NodesDiagnosticMetricID = "nodesCPUMetrics"
)

// diagnostic definitions mapping holds all of the diagnostic definitions that can be used for collector metrics diagnostics
var diagnosticDefinitions map[string]*diagnosticDefinition = map[string]*diagnosticDefinition{
	NodesDiagnosticMetricID: {
		ID:          NodesDiagnosticMetricID,
		MetricName:  "kube_node_status_capacity_cpu_cores",
		Label:       "Node CPU cores capacity is being scraped",
		Description: "Determine if the node CPU cores capacity is being scraped",
	},

	KubecostDiagnosticMetricID: {
		ID:          KubecostDiagnosticMetricID,
		MetricName:  "node_total_hourly_cost",
		Label:       "Kubecost metrics for a node are being scraped",
		Description: "Determine if kubecost metrics for a node are being scraped",
	},
}

// diagnosticsResults stores the current state of diagnostic results
var diagnosticsResults map[string]*diagnosticsResult = make(map[string]*diagnosticsResult)

type diagnosticDefinition struct {
	ID          string
	MetricName  string
	Label       string
	Description string
	DocLink     string
}

type diagnosticsResult struct {
	Result map[string]any
	Passed bool
}

type DiagnosticsModule struct {
	lock    sync.RWMutex
	updater Updater
}

func NewDiagnosticsModule(updater Updater) *DiagnosticsModule {
	// Initialize diagnostics results to false to represent that no data has been collected yet
	for id := range diagnosticDefinitions {
		diagnosticsResults[id] = &diagnosticsResult{
			Result: make(map[string]any),
			Passed: false,
		}
	}

	return &DiagnosticsModule{
		updater: updater,
	}
}

func (d *DiagnosticsModule) Update(updateSet *UpdateSet) {
	if updateSet == nil {
		return
	}

	// Create a deep copy for the async update to avoid race condition
	updateSetCopy := &UpdateSet{
		Timestamp: updateSet.Timestamp,
		Updates:   make([]Update, len(updateSet.Updates)),
	}
	copy(updateSetCopy.Updates, updateSet.Updates)

	// Run update asynchronously
	go func() {
		d.updater.Update(updateSetCopy)
	}()

	d.lock.Lock()
	defer d.lock.Unlock()

	timestamp := updateSet.Timestamp.String()
	for id, dd := range diagnosticDefinitions {
		for _, update := range updateSet.Updates {
			if update.Name == dd.MetricName {
				if len(diagnosticsResults[id].Result) == 0 {
					// For the first UpdateSet received for that metric, we default to true. If we later miss the metric for a timestamp, it will be set to false.
					diagnosticsResults[id].Passed = true
				}
				diagnosticsResults[id].Result[timestamp] = update.Value
			}
		}
		if diagnosticsResults[id].Result[timestamp] == nil {
			diagnosticsResults[id].Passed = false
		}
	}
}

func (d *DiagnosticsModule) DiagnosticsDefinitions() map[string]*diagnosticDefinition {
	return diagnosticDefinitions
}

func (d *DiagnosticsModule) DiagnosticsDetails(diagnosticsId string) (map[string]any, error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	if _, exists := diagnosticDefinitions[diagnosticsId]; !exists {
		return nil, fmt.Errorf("diagnostic ID: %s not found", diagnosticsId)
	}

	details := map[string]any{
		"query":   diagnosticDefinitions[diagnosticsId].MetricName,
		"label":   diagnosticDefinitions[diagnosticsId].Label,
		"docLink": diagnosticDefinitions[diagnosticsId].DocLink,
		"result":  maps.Clone(diagnosticsResults[diagnosticsId].Result),
		"passed":  diagnosticsResults[diagnosticsId].Passed,
	}
	// reset the result and passed for the next run
	diagnosticsResults[diagnosticsId].Result = make(map[string]any)
	diagnosticsResults[diagnosticsId].Passed = false
	return details, nil
}
