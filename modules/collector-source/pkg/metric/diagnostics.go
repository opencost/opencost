package metric

import (
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/kubecost/events"
	"github.com/opencost/opencost/core/pkg/collections"
	"github.com/opencost/opencost/core/pkg/util/sliceutil"
	"github.com/opencost/opencost/modules/collector-source/pkg/event"
)

// Collector Metric Diagnostic IDs
const (
	// OpencostDiagnosticMetricID is the identifier for the metric used to determine if Opencost metrics are being updated
	OpencostDiagnosticMetricID = "opencostMetric"

	// NodesDiagnosticMetricID is the identifier for the query used to determine if the node CPU cores capacity is being updated
	NodesDiagnosticMetricID = "nodesCPUMetrics"

	// DcgmScraperDiagnosticID contains the identifier for the the DCGM scraper diagnostic.
	DcgmScraperDiagnosticID = event.DCGMScraperName

	// OpenCostScraperDiagnosticID contains the identifier for the the opencost metrics scraper diagnostic
	OpenCostScraperDiagnosticID = event.OpenCostScraperName

	// NodeStatsScraperDiagnosticID contains the identifier for the the node stats summary scraper diagnostic
	NodeStatsScraperDiagnosticID = event.NodeStatsScraperName

	// NetworkCostsScraperDiagnosticID contains the identifier for the the network-costs scraper diagnostic.
	NetworkCostsScraperDiagnosticID = event.NetworkCostsScraperName

	// Kubernetes scrapers contains the identifiers for all the specific KubernetesCluster scrapers.
	KubernetesNodesScraperDiagnosticID        = event.KubernetesClusterScraperName + "-" + event.NodeScraperType
	KubernetesNamespacesScraperDiagnosticID   = event.KubernetesClusterScraperName + "-" + event.NamespaceScraperType
	KubernetesReplicaSetsScraperDiagnosticID  = event.KubernetesClusterScraperName + "-" + event.ReplicaSetScraperType
	KubernetesDeploymentsScraperDiagnosticID  = event.KubernetesClusterScraperName + "-" + event.DeploymentScraperType
	KubernetesStatefulSetsScraperDiagnosticID = event.KubernetesClusterScraperName + "-" + event.StatefulSetScraperType
	KubernetesServicesScraperDiagnosticID     = event.KubernetesClusterScraperName + "-" + event.ServiceScraperType
	KubernetesPodsScraperDiagnosticID         = event.KubernetesClusterScraperName + "-" + event.PodScraperType
	KubernetesPvsScraperDiagnosticID          = event.KubernetesClusterScraperName + "-" + event.PvScraperType
	KubernetesPvcsScraperDiagnosticID         = event.KubernetesClusterScraperName + "-" + event.PvcScraperType
)

// DiagnosticType is used in the definitions to determine which type of implementation to use when representing the
// diagnostic
type DiagnosticType int

const (
	DiagnosticTypeMetric  DiagnosticType = 0
	DiagnosticTypeScraper DiagnosticType = 1
	// more diagnostic types?
)

// diagnostic defintion is the type used to define a deterministic list of specific diagnostics we _expect_ to collect
type diagnosticDefinition struct {
	ID          string
	MetricName  string
	Label       string
	Description string
	DocLink     string
	DiagType    DiagnosticType
}

// diagnostic definitions mapping holds all of the diagnostic definitions that can be used for collector metrics diagnostics
var diagnosticDefinitions map[string]*diagnosticDefinition = map[string]*diagnosticDefinition{
	NodesDiagnosticMetricID: {
		ID:          NodesDiagnosticMetricID,
		MetricName:  KubeNodeStatusCapacityCPUCores,
		Label:       "Node CPU cores capacity is being scraped",
		Description: "Determine if the node CPU cores capacity metrics are being updated",
		DiagType:    DiagnosticTypeMetric,
	},

	OpencostDiagnosticMetricID: {
		ID:          OpencostDiagnosticMetricID,
		MetricName:  NodeTotalHourlyCost,
		Label:       "Opencost metrics for a node are being scraped",
		Description: "Determine if opencost metrics for a node are being updated",
		DiagType:    DiagnosticTypeMetric,
	},

	DcgmScraperDiagnosticID: {
		ID:          DcgmScraperDiagnosticID,
		MetricName:  event.DCGMScraperName,
		Label:       "DCGM scraper is available and is being scraped.",
		Description: scraperDiagnosticDescriptionFor(event.DCGMScraperName, ""),
		DiagType:    DiagnosticTypeScraper,
	},

	OpenCostScraperDiagnosticID: {
		ID:          OpenCostScraperDiagnosticID,
		MetricName:  event.OpenCostScraperName,
		Label:       "Opencost metrics scraper is available and is being scraped.",
		Description: scraperDiagnosticDescriptionFor(event.OpenCostScraperName, ""),
		DiagType:    DiagnosticTypeScraper,
	},

	NodeStatsScraperDiagnosticID: {
		ID:          NodeStatsScraperDiagnosticID,
		MetricName:  event.NodeStatsScraperName,
		Label:       "Node stats summary scraper is available and is being scraped.",
		Description: scraperDiagnosticDescriptionFor(event.NodeStatsScraperName, ""),
		DiagType:    DiagnosticTypeScraper,
	},

	NetworkCostsScraperDiagnosticID: {
		ID:          NetworkCostsScraperDiagnosticID,
		MetricName:  event.NetworkCostsScraperName,
		Label:       "Network costs daemonset metrics scrapers are available and being scraped.",
		Description: scraperDiagnosticDescriptionFor(event.NetworkCostsScraperName, ""),
		DiagType:    DiagnosticTypeScraper,
	},

	KubernetesNodesScraperDiagnosticID: {
		ID:          KubernetesNodesScraperDiagnosticID,
		MetricName:  KubernetesNodesScraperDiagnosticID,
		Label:       fmt.Sprintf("Kubernetes cluster resources: %s are available and being scraped", event.NodeScraperType),
		Description: scraperDiagnosticDescriptionFor(event.KubernetesClusterScraperName, event.NodeScraperType),
		DiagType:    DiagnosticTypeScraper,
	},

	KubernetesNamespacesScraperDiagnosticID: {
		ID:          KubernetesNamespacesScraperDiagnosticID,
		MetricName:  KubernetesNamespacesScraperDiagnosticID,
		Label:       fmt.Sprintf("Kubernetes cluster resources: %s are available and being scraped", event.NamespaceScraperType),
		Description: scraperDiagnosticDescriptionFor(event.KubernetesClusterScraperName, event.NamespaceScraperType),
		DiagType:    DiagnosticTypeScraper,
	},

	KubernetesReplicaSetsScraperDiagnosticID: {
		ID:          KubernetesReplicaSetsScraperDiagnosticID,
		MetricName:  KubernetesReplicaSetsScraperDiagnosticID,
		Label:       fmt.Sprintf("Kubernetes cluster resources: %s are available and being scraped", event.ReplicaSetScraperType),
		Description: scraperDiagnosticDescriptionFor(event.KubernetesClusterScraperName, event.ReplicaSetScraperType),
		DiagType:    DiagnosticTypeScraper,
	},

	KubernetesDeploymentsScraperDiagnosticID: {
		ID:          KubernetesDeploymentsScraperDiagnosticID,
		MetricName:  KubernetesDeploymentsScraperDiagnosticID,
		Label:       fmt.Sprintf("Kubernetes cluster resources: %s are available and being scraped", event.DeploymentScraperType),
		Description: scraperDiagnosticDescriptionFor(event.KubernetesClusterScraperName, event.DeploymentScraperType),
		DiagType:    DiagnosticTypeScraper,
	},

	KubernetesStatefulSetsScraperDiagnosticID: {
		ID:          KubernetesStatefulSetsScraperDiagnosticID,
		MetricName:  KubernetesStatefulSetsScraperDiagnosticID,
		Label:       fmt.Sprintf("Kubernetes cluster resources: %s are available and being scraped", event.StatefulSetScraperType),
		Description: scraperDiagnosticDescriptionFor(event.KubernetesClusterScraperName, event.StatefulSetScraperType),
		DiagType:    DiagnosticTypeScraper,
	},

	KubernetesServicesScraperDiagnosticID: {
		ID:          KubernetesServicesScraperDiagnosticID,
		MetricName:  KubernetesServicesScraperDiagnosticID,
		Label:       fmt.Sprintf("Kubernetes cluster resources: %s are available and being scraped", event.ServiceScraperType),
		Description: scraperDiagnosticDescriptionFor(event.KubernetesClusterScraperName, event.ServiceScraperType),
		DiagType:    DiagnosticTypeScraper,
	},

	KubernetesPodsScraperDiagnosticID: {
		ID:          KubernetesPodsScraperDiagnosticID,
		MetricName:  KubernetesPodsScraperDiagnosticID,
		Label:       fmt.Sprintf("Kubernetes cluster resources: %s are available and being scraped", event.PodScraperType),
		Description: scraperDiagnosticDescriptionFor(event.KubernetesClusterScraperName, event.PodScraperType),
		DiagType:    DiagnosticTypeScraper,
	},

	KubernetesPvsScraperDiagnosticID: {
		ID:          KubernetesPvsScraperDiagnosticID,
		MetricName:  KubernetesPvsScraperDiagnosticID,
		Label:       fmt.Sprintf("Kubernetes cluster resources: %s are available and being scraped", event.PvScraperType),
		Description: scraperDiagnosticDescriptionFor(event.KubernetesClusterScraperName, event.PvScraperType),
		DiagType:    DiagnosticTypeScraper,
	},

	KubernetesPvcsScraperDiagnosticID: {
		ID:          KubernetesPvcsScraperDiagnosticID,
		MetricName:  KubernetesPvcsScraperDiagnosticID,
		Label:       fmt.Sprintf("Kubernetes cluster resources: %s are available and being scraped", event.PvcScraperType),
		Description: scraperDiagnosticDescriptionFor(event.KubernetesClusterScraperName, event.PvcScraperType),
		DiagType:    DiagnosticTypeScraper,
	},
}

// scraper identifier for diagnostic mapping _must_ match diagnostic ids defined above
func scraperIdFor(scraperName, scrapeType string) string {
	if scrapeType == "" {
		return scraperName
	}
	return fmt.Sprintf("%s-%s", scraperName, scrapeType)
}

// helper for generating dynamic scraper events diagnostic descriptions
func scraperDiagnosticDescriptionFor(scraperName, scrapeType string) string {
	if scrapeType == "" {
		return fmt.Sprintf("Determine if the scraper for: %s is correctly reporting data", scraperName)
	}
	return fmt.Sprintf("Determine if the scraper for: %s is correctly report data for type: %s", scraperName, scrapeType)
}

// CollectorDiagnostic is a basic interface used to allow various types of diagnostic data collection
type CollectorDiagnostic interface {
	// Id returns the identifier for the diagnostic
	Id() string

	// Name returns the name of the metric being run
	Name() string

	// Details generates an exportable detail map for the specific diagnostic, and resets any of its internal
	// state for the current cycle.
	Details() map[string]any
}

// metric diagnostic is checked on metrics update -- it maintains a historic record of all the instants
// a specific metric was updated, and reports a diagnostic on the validity of that history.
type metricDiagnostic struct {
	diagnostic       *diagnosticDefinition
	updateTimestamps []time.Time
	result           map[string]float64
}

// creates a new metric diagnostic
func newMetricDiagnostic(diagnostic *diagnosticDefinition) *metricDiagnostic {
	return &metricDiagnostic{
		diagnostic: diagnostic,
		result:     make(map[string]float64),
	}
}

// Id returns the identifier for the metric diagnostic type -- this just proxies from the diagnostic
// definition.
func (md *metricDiagnostic) Id() string {
	return md.diagnostic.ID
}

// Name returns the name of the metric being run for the metric diagnostic type -- this just proxies from
// the diagnostic definition.
func (md *metricDiagnostic) Name() string {
	return md.diagnostic.MetricName
}

// Details generates an exportable detail map for the specific diagnostic, and resets any of its internal
// state for the current cycle.
func (md *metricDiagnostic) Details() map[string]any {
	// for all timestamps that occurred during our update cycle,
	// if any timestamps for our metric do not exist, then we
	// say that the diagnostic failed. if there are no timestamps
	// marked in the result, then we also say the diagnostic failed.
	passed := true
	if len(md.result) == 0 {
		passed = false
	} else {
		for _, t := range md.updateTimestamps {
			key := t.Format(time.RFC3339)

			_, hasTimestamp := md.result[key]
			if !hasTimestamp {
				passed = false
				break
			}
		}
	}

	details := map[string]any{
		"query":   md.Name(),
		"label":   md.diagnostic.Label,
		"docLink": md.diagnostic.DocLink,
		"result":  maps.Clone(md.result),
		"passed":  passed,
	}

	// reset the update timestamps and results
	md.updateTimestamps = []time.Time{}
	for k := range md.result {
		delete(md.result, k)
	}

	return details
}

// scrapeDiagnostic maintains the latest state of each scrape event that occurs. scrape
// events can be registered for any event, but only the specific scrapes with diagnostic
// definitions defined will export as diagnostics.
type scrapeDiagnostic struct {
	diagnostic *diagnosticDefinition
	scraper    string
	scrapeType string
	targets    int
	errors     []error
}

// creates a new scrape diagnostic from the event data and diagnostics definition
func newScrapeDiagnostic(
	scrapeEvent event.ScrapeEvent,
	definition *diagnosticDefinition,
) *scrapeDiagnostic {
	return &scrapeDiagnostic{
		diagnostic: definition,
		scraper:    scrapeEvent.ScraperName,
		scrapeType: scrapeEvent.ScrapeType,
		targets:    scrapeEvent.Targets,
		errors:     scrapeEvent.Errors,
	}
}

// Id is a concatenation of scraper and scrapeType if a scrapeType exists.
func (sd *scrapeDiagnostic) Id() string {
	if sd.diagnostic != nil {
		return sd.diagnostic.ID
	}
	return scraperIdFor(sd.scraper, sd.scrapeType)
}

// Name returns the name of the scraper the event fired from.
func (sd *scrapeDiagnostic) Name() string {
	return sd.scraper
}

// Details generates an exportable detail map for the specific diagnostic, and resets any of its internal
// state for the current cycle.
func (sd *scrapeDiagnostic) Details() map[string]any {
	// passed if there are no errors
	passed := len(sd.errors) == 0

	// map errors to a string slice for easier propagation
	var errs []string
	if !passed {
		errs = sliceutil.Map(sd.errors, func(e error) string { return e.Error() })
	} else {
		errs = []string{}
	}

	// since a scrape event does not require a matching diagnostic definition,
	// we must generate properties normally extracted from the defintiion
	var label string
	if sd.diagnostic != nil {
		label = sd.diagnostic.Label
	} else {
		label = fmt.Sprintf("%s scraper is available and being scraped.", sd.scraper)
	}

	// same for doclink
	var docLink string
	if sd.diagnostic != nil {
		docLink = sd.diagnostic.DocLink
	} else {
		docLink = ""
	}

	details := map[string]any{
		// stats contains total entities to scrape, success (of the total), and failures (of the total)
		"stats": map[string]any{
			"total":   sd.targets,
			"success": max(sd.targets-len(errs), 0),
			"fail":    len(errs),
		},
		"label":   label,
		"docLink": docLink,
		"errors":  errs,
		"passed":  passed,
	}

	// scraper diagnostics do not maintain any internal/historical state
	// to reset -- it just maintains the most recent data. if we decide
	// to track historical event data, would need to reset the state after
	// this call.

	return details
}

// DiagnosticsModule is a helper type for managing all of the internal diagnostics for the collector datasource.
type DiagnosticsModule struct {
	lock            sync.RWMutex
	diagnostics     *collections.IdNameMap[CollectorDiagnostic]
	updater         Updater
	scrapeHandlerId events.HandlerID // scrape event handler identifier for removal
}

// NewDiagnosticsModule creates a new `DiagnosticsModule` instance to be used with a collector data source
func NewDiagnosticsModule(updater Updater) *DiagnosticsModule {
	// initialize all metric diagnostics IFF the diagnostic type is "metrics"
	// NOTE: scraper diagnostics are dynamically created as scrape results arrive
	diagnostics := collections.NewIdNameMap[CollectorDiagnostic]()
	for _, def := range diagnosticDefinitions {
		// only insert metric diagnostic types
		if def.DiagType == DiagnosticTypeMetric {
			diagnostics.Insert(newMetricDiagnostic(def))
		}
	}

	dm := &DiagnosticsModule{
		diagnostics: diagnostics,
		updater:     updater,
	}

	scrapeEvents := events.GlobalDispatcherFor[event.ScrapeEvent]()
	dm.scrapeHandlerId = scrapeEvents.AddEventHandler(dm.onScrapeEvent)

	return dm
}

// handles a scrape event dispatched -- updates the record for the specific scrape
// diagnostic.
func (d *DiagnosticsModule) onScrapeEvent(event event.ScrapeEvent) {
	d.lock.Lock()
	defer d.lock.Unlock()

	id := scraperIdFor(event.ScraperName, event.ScrapeType)

	// scrape events can occur without a backing diagnostic definition -- just
	// ignore if this happens
	def, ok := diagnosticDefinitions[id]
	if !ok {
		return
	}

	d.diagnostics.Insert(newScrapeDiagnostic(event, def))
}

func (d *DiagnosticsModule) Update(updateSet *UpdateSet) {
	if updateSet == nil {
		return
	}

	// This is done so that the update func is marked complete when both the updater and diagnostics are done
	// Otherwise we might face a race condition when calling the diagnostics details func before the diagnostics are done
	var wg sync.WaitGroup
	wg.Add(2) // 1 for updater, 1 for diagnostics

	go func() {
		defer wg.Done()

		d.lock.Lock()
		defer d.lock.Unlock()

		// add the timestamp to all metric diagnostic instances (see notes on addUpdateTimestamp)
		ts := updateSet.Timestamp
		d.addUpdateTimestamp(ts)

		timestamp := ts.Format(time.RFC3339)

		for _, update := range updateSet.Updates {
			if metric, ok := d.diagnostics.ByName(update.Name); ok {
				// this is unfortunately necessary due to the way our diangostic collectors
				// differ in functionality -- it makes more sense to duck type here rather
				// than maintain a separate map of just the metric types, or add metric
				// specific implementation details to the CollectorDiagnostic interface.
				// generally, we _should_ be able to make this assertion -- but we'll check in case.
				if metricDiag, isType := metric.(*metricDiagnostic); isType {
					// mark the timestamp as "seen" with the value
					metricDiag.result[timestamp] = update.Value
				}
			}
		}
	}()

	// We are still maintaining the order in which the updates to the repo are called
	// as this function gets the new call only when both these go routines are done
	go func() {
		defer wg.Done()
		d.updater.Update(updateSet)
	}()

	wg.Wait()
}

// appends an update timestamp on each of the metric diagnostics -- we need to write
// every timestamp that the update makes unfortunately. There isn't a way to determine
// if a diagnostic service "cycle" is complete, so it's not really possible to maintain
// a most recent timestamps on the DiagnosticsModule (the optimal solution). we're not
// far from a solid design here, just might need some more support on the diagnostic
// service side.
func (d *DiagnosticsModule) addUpdateTimestamp(t time.Time) {
	for _, def := range diagnosticDefinitions {
		if def.DiagType != DiagnosticTypeMetric {
			continue
		}

		diag, ok := d.diagnostics.ById(def.ID)
		if !ok {
			continue
		}

		// More duck typing sadly -- there are some fundamental design incompatibilities
		// with the way DiagnosticService was written and this cached diagnostic approach
		// that make things like "cycle" resets a bit difficult
		if metricDiag, ok := diag.(*metricDiagnostic); ok {
			metricDiag.updateTimestamps = append(metricDiag.updateTimestamps, t)
		}
	}
}

// DiagnosticDefinitions returns a deterministic mapping of pre-defined diagnostics used with the collector.
func (d *DiagnosticsModule) DiagnosticsDefinitions() map[string]*diagnosticDefinition {
	return diagnosticDefinitions
}

// DiagnosticDetails returns the latest details for the diagnostic type
func (d *DiagnosticsModule) DiagnosticsDetails(diagnosticsId string) (map[string]any, error) {
	d.lock.RLock()
	defer d.lock.RUnlock()

	// If a bogus diagnostics id was passed, we can check the definitions first
	if _, exists := diagnosticDefinitions[diagnosticsId]; !exists {
		return nil, fmt.Errorf("invalid diagnostic id: %s not found", diagnosticsId)
	}

	// for some diagnostics, like the scraper variant, they may not have been registered
	// yet (no scrape events), so we should return an error indicating that the scrape
	// hasn't occurred yet
	diagnostic, exists := d.diagnostics.ById(diagnosticsId)
	if !exists {
		return nil, fmt.Errorf("diagnostic not available: %s", diagnosticsId)
	}

	return diagnostic.Details(), nil
}
