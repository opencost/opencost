package event

const (
	DCGMScraperName              = "dcgm-metrics"
	OpenCostScraperName          = "opencost-metrics"
	NodeStatsScraperName         = "nodestats-metrics"
	NetworkCostsScraperName      = "network-costs-metrics"
	KubernetesClusterScraperName = "kubernetes-metrics"
)

const (
	NodeScraperType          = "nodes"
	NamespaceScraperType     = "namespaces"
	ReplicaSetScraperType    = "replicasets"
	DeploymentScraperType    = "deployments"
	StatefulSetScraperType   = "statefulsets"
	ServiceScraperType       = "services"
	PodScraperType           = "pods"
	PvScraperType            = "pvs"
	PvcScraperType           = "pvcs"
	ResourceQuotaScraperType = "resourcequotas"
)

// ScrapeEvent is dispatched when a scrape is performed over a set of targets. It contains the name
// of the scraper performing the scrape, the total number of targets, and any errors encountered.
type ScrapeEvent struct {
	// The name of the actual Scraper implementation performing the target scrapes.
	ScraperName string

	// The type of scrape being performed. For example, if a scraper performs multiple scrapes
	// for different resources, this field can be used to distinguish between them.
	ScrapeType string

	// The total number of targets being accessed by the scraper.
	Targets int

	// Any errors that occurred during the scrape.
	Errors []error
}
