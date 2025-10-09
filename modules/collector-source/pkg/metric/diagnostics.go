package metric

import (
	"fmt"
	"sync"

	"github.com/kubecost/events"
	"github.com/opencost/opencost/core/pkg/collections"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/sliceutil"
	"github.com/opencost/opencost/modules/collector-source/pkg/event"
)

// Collector Metric Diagnostic IDs
const (
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

	// Metric Names for the diagnostics (used in the UI)
	DGGMScraperDiagnosticMetricName                   = "DCGM Metrics"
	OpenCostScraperDiagnosticMetricName               = "Opencost Metrics"
	NodeStatsScraperDiagnosticMetricName              = "Node Stats Metrics"
	NetworkCostsScraperDiagnosticMetricName           = "Network Costs Metrics"
	KubernetesNodesScraperDiagnosticMetricName        = "Kubernetes Nodes Metrics"
	KubernetesNamespacesScraperDiagnosticMetricName   = "Kubernetes Namespaces Metrics"
	KubernetesReplicaSetsScraperDiagnosticMetricName  = "Kubernetes Replica Sets Metrics"
	KubernetesDeploymentsScraperDiagnosticMetricName  = "Kubernetes Deployments Metrics"
	KubernetesStatefulSetsScraperDiagnosticMetricName = "Kubernetes Stateful Sets Metrics"
	KubernetesServicesScraperDiagnosticMetricName     = "Kubernetes Services Metrics"
	KubernetesPodsScraperDiagnosticMetricName         = "Kubernetes Pods Metrics"
	KubernetesPvsScraperDiagnosticMetricName          = "Kubernetes PVs Metrics"
	KubernetesPvcsScraperDiagnosticMetricName         = "Kubernetes PVCs Metrics"
)

// diagnostic defintion is the type used to define a deterministic list of specific diagnostics we _expect_ to collect
type diagnosticDefinition struct {
	ID          string
	MetricName  string
	Label       string
	Description string
	DocLink     string
}

// diagnostic definitions mapping holds all of the diagnostic definitions that can be used for collector metrics diagnostics
var diagnosticDefinitions map[string]*diagnosticDefinition = map[string]*diagnosticDefinition{
	DcgmScraperDiagnosticID: {
		ID:          DcgmScraperDiagnosticID,
		MetricName:  DGGMScraperDiagnosticMetricName,
		Label:       "DCGM scraper is available and is being scraped.",
		Description: scraperDiagnosticDescriptionFor(event.DCGMScraperName, ""),
	},

	OpenCostScraperDiagnosticID: {
		ID:          OpenCostScraperDiagnosticID,
		MetricName:  OpenCostScraperDiagnosticMetricName,
		Label:       "Opencost metrics scraper is available and is being scraped.",
		Description: scraperDiagnosticDescriptionFor(event.OpenCostScraperName, ""),
	},

	NodeStatsScraperDiagnosticID: {
		ID:          NodeStatsScraperDiagnosticID,
		MetricName:  NodeStatsScraperDiagnosticMetricName,
		Label:       "Node stats summary scraper is available and is being scraped.",
		Description: scraperDiagnosticDescriptionFor(event.NodeStatsScraperName, ""),
	},

	NetworkCostsScraperDiagnosticID: {
		ID:          NetworkCostsScraperDiagnosticID,
		MetricName:  NetworkCostsScraperDiagnosticMetricName,
		Label:       "Network costs daemonset metrics scrapers are available and being scraped.",
		Description: scraperDiagnosticDescriptionFor(event.NetworkCostsScraperName, ""),
	},

	KubernetesNodesScraperDiagnosticID: {
		ID:          KubernetesNodesScraperDiagnosticID,
		MetricName:  KubernetesNodesScraperDiagnosticMetricName,
		Label:       fmt.Sprintf("Kubernetes cluster resources: %s are available and being scraped", event.NodeScraperType),
		Description: scraperDiagnosticDescriptionFor(event.KubernetesClusterScraperName, event.NodeScraperType),
	},

	KubernetesNamespacesScraperDiagnosticID: {
		ID:          KubernetesNamespacesScraperDiagnosticID,
		MetricName:  KubernetesNamespacesScraperDiagnosticMetricName,
		Label:       fmt.Sprintf("Kubernetes cluster resources: %s are available and being scraped", event.NamespaceScraperType),
		Description: scraperDiagnosticDescriptionFor(event.KubernetesClusterScraperName, event.NamespaceScraperType),
	},

	KubernetesReplicaSetsScraperDiagnosticID: {
		ID:          KubernetesReplicaSetsScraperDiagnosticID,
		MetricName:  KubernetesReplicaSetsScraperDiagnosticMetricName,
		Label:       fmt.Sprintf("Kubernetes cluster resources: %s are available and being scraped", event.ReplicaSetScraperType),
		Description: scraperDiagnosticDescriptionFor(event.KubernetesClusterScraperName, event.ReplicaSetScraperType),
	},

	KubernetesDeploymentsScraperDiagnosticID: {
		ID:          KubernetesDeploymentsScraperDiagnosticID,
		MetricName:  KubernetesDeploymentsScraperDiagnosticMetricName,
		Label:       fmt.Sprintf("Kubernetes cluster resources: %s are available and being scraped", event.DeploymentScraperType),
		Description: scraperDiagnosticDescriptionFor(event.KubernetesClusterScraperName, event.DeploymentScraperType),
	},

	KubernetesStatefulSetsScraperDiagnosticID: {
		ID:          KubernetesStatefulSetsScraperDiagnosticID,
		MetricName:  KubernetesStatefulSetsScraperDiagnosticMetricName,
		Label:       fmt.Sprintf("Kubernetes cluster resources: %s are available and being scraped", event.StatefulSetScraperType),
		Description: scraperDiagnosticDescriptionFor(event.KubernetesClusterScraperName, event.StatefulSetScraperType),
	},

	KubernetesServicesScraperDiagnosticID: {
		ID:          KubernetesServicesScraperDiagnosticID,
		MetricName:  KubernetesServicesScraperDiagnosticMetricName,
		Label:       fmt.Sprintf("Kubernetes cluster resources: %s are available and being scraped", event.ServiceScraperType),
		Description: scraperDiagnosticDescriptionFor(event.KubernetesClusterScraperName, event.ServiceScraperType),
	},

	KubernetesPodsScraperDiagnosticID: {
		ID:          KubernetesPodsScraperDiagnosticID,
		MetricName:  KubernetesPodsScraperDiagnosticMetricName,
		Label:       fmt.Sprintf("Kubernetes cluster resources: %s are available and being scraped", event.PodScraperType),
		Description: scraperDiagnosticDescriptionFor(event.KubernetesClusterScraperName, event.PodScraperType),
	},

	KubernetesPvsScraperDiagnosticID: {
		ID:          KubernetesPvsScraperDiagnosticID,
		MetricName:  KubernetesPvsScraperDiagnosticMetricName,
		Label:       fmt.Sprintf("Kubernetes cluster resources: %s are available and being scraped", event.PvScraperType),
		Description: scraperDiagnosticDescriptionFor(event.KubernetesClusterScraperName, event.PvScraperType),
	},

	KubernetesPvcsScraperDiagnosticID: {
		ID:          KubernetesPvcsScraperDiagnosticID,
		MetricName:  KubernetesPvcsScraperDiagnosticMetricName,
		Label:       fmt.Sprintf("Kubernetes cluster resources: %s are available and being scraped", event.PvcScraperType),
		Description: scraperDiagnosticDescriptionFor(event.KubernetesClusterScraperName, event.PvcScraperType),
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
	if sd.diagnostic != nil {
		return sd.diagnostic.MetricName
	}
	return scraperIdFor(sd.scraper, sd.scrapeType)
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
	scrapeHandlerId events.HandlerID // scrape event handler identifier for removal
}

// NewDiagnosticsModule creates a new `DiagnosticsModule` instance to be used with a collector data source
func NewDiagnosticsModule() *DiagnosticsModule {
	diagnostics := collections.NewIdNameMap[CollectorDiagnostic]()
	dm := &DiagnosticsModule{
		diagnostics: diagnostics,
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

	err := d.diagnostics.Insert(newScrapeDiagnostic(event, def))
	if err != nil {
		log.Errorf("failed to insert scrape diagnostic: %s", err)
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
