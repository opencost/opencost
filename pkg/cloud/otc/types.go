package otc

import (
	"sync"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/pkg/cloud/models"
)

type OTCStats struct {
	CurrentPage    int `json:"currentPage"`
	MaxPages       int `json:"maxPages"`
	RecordsPerPage int `json:"recordsPerPage"`
}

type Product struct {
	ProductIdParameter string `json:"productIdParameter"`
	OpiFlavour         string `json:"opiFlavour"`
	OsUnit             string `json:"osUnit,omitempty"`
	PriceAmount        string `json:"priceAmount"`
	VCpu               string `json:"vCpu,omitempty"`
	Ram                string `json:"ram,omitempty"`
}

// OTC node pricing attributes
type OTCNodeAttributes struct {
	Type  string // like s2.large.1
	OS    string // like windows
	Price string // (in EUR) like 0.023
	RAM   string // (in GB) like 2
	VCPU  string // like 8
}

type OTCPVAttributes struct {
	Type  string // like vss.ssd
	Price string // (in EUR/GB/h) like 0.01
}

// OTC pricing is either for a node, a persistent volume (or a database, network, cluster, ...)
type OTCPricing struct {
	NodeAttributes *OTCNodeAttributes
	PVAttributes   *OTCPVAttributes
}

// the main provider struct
type OTC struct {
	Clientset               clustercache.ClusterCache
	Pricing                 map[string]*OTCPricing
	Config                  models.ProviderConfig
	ClusterRegion           string
	projectID               string
	clusterManagementPrice  float64
	BaseCPUPrice            string
	BaseRAMPrice            string
	BaseGPUPrice            string
	ValidPricingKeys        map[string]bool
	DownloadPricingDataLock sync.RWMutex
}
