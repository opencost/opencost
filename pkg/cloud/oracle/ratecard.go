package oracle

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/pkg/cloud/httputil"
	"github.com/opencost/opencost/pkg/cloud/models"
)

// 1 month = 744 hours by price list documentation.
const hoursPerMonth = 24 * 31

type RateCardStore struct {
	shapesEndpoint string
	url            string
	currencyCode   string
	client         *http.Client
	prices         map[string]Price
}

type Price struct {
	ProductName string
	Metric      string
	Model       string
	UnitPrice   float64
}

type PricingResponse struct {
	Items []Item `json:"items"`
}

type Item struct {
	PartNumber                string `json:"partNumber"`
	DisplayName               string `json:"displayName"`
	MetricName                string `json:"metricName"`
	ServiceCategory           string `json:"serviceCategory"`
	CurrencyCodeLocalizations []struct {
		CurrencyCode string `json:"currencyCode"`
		Prices       []struct {
			Model string  `json:"model"`
			Value float64 `json:"value"`
		} `json:"prices"`
	} `json:"currencyCodeLocalizations"`
	Description string `json:"description,omitempty"`
}

func NewRateCardStore(url, currencyCode string) *RateCardStore {
	return &RateCardStore{
		url:          url,
		currencyCode: currencyCode,
		// Zero-value http.Client has no timeout; use the shared bounded client so
		// a stalled rate-card endpoint can't hang ingestion.
		client: httputil.BoundedClient(),
		prices: map[string]Price{},
	}
}

func (rcs *RateCardStore) ForLB(defaultPricing DefaultPricing) (*models.LoadBalancer, error) {
	var cost float64
	rc, ok := rcs.prices[loadBalancerPartNumber]
	if ok {
		cost = rc.UnitPrice
	} else {
		c, err := strconv.ParseFloat(defaultPricing.LB, 64)
		if err != nil {
			return nil, err
		}
		cost = c
	}
	return &models.LoadBalancer{
		Cost: cost,
	}, nil
}

func (rcs *RateCardStore) ForManagedCluster(clusterType string) float64 {
	// Basic clusters are free, and do not have a rate card.
	if clusterType == "BASIC_CLUSTER" {
		return 0.0
	}
	// Enhanced clusters have a rate card, and require a pricing look up.
	rc, ok := rcs.prices[enhancedClusterPartNumber]
	if !ok {
		return 0.1
	}
	return rc.UnitPrice
}

func (rcs *RateCardStore) ForEgressRegion(region string, defaultPricing DefaultPricing) (*models.Network, error) {
	pn := egressRegionPartNumber(region)
	var egressCost float64
	if rc, ok := rcs.prices[pn]; ok {
		egressCost = rc.UnitPrice
	} else if defaultPricing.Egress != "" {
		cost, err := strconv.ParseFloat(defaultPricing.Egress, 64)
		if err != nil {
			return nil, err
		}
		egressCost = cost
	}
	return &models.Network{
		ZoneNetworkEgressCost:     0,
		RegionNetworkEgressCost:   egressCost,
		InternetNetworkEgressCost: egressCost,
		NatGatewayEgressCost:      0,
		NatGatewayIngressCost:     0,
	}, nil
}

// ForPVK retrieves a Gb/Hour cost for a given PVKey.
func (rcs *RateCardStore) ForPVK(pvk models.PVKey, defaultPricing DefaultPricing) (*models.PV, error) {
	features := pvk.Features()
	rc, ok := rcs.prices[features]
	if !ok {
		// Use default storage if no pricing found
		return &models.PV{
			Cost: defaultPricing.Storage,
		}, nil
	}
	monthly := rc.UnitPrice // Oracle unit pricing for storage is in Gb/Month
	if features == blockVolumePartNumber {
		// A block volume is billed for its performance units as well as its capacity.
		monthly += vpusPerGB(pvk) * rcs.prices[blockVolumePerformancePartNumber].UnitPrice
	}
	return &models.PV{
		// Full precision: a GB-hour price is a few hundred-thousandths, and six decimals would
		// round it by up to 1.5 %.
		Cost: strconv.FormatFloat(monthly/hoursPerMonth, 'f', -1, 64),
	}, nil
}

// vpusPerGB is the volume performance units per GB the PV's storage class sets.
func vpusPerGB(pvk models.PVKey) float64 {
	if k, ok := pvk.(*oraclePVKey); ok {
		if v, ok := k.parameters["vpusPerGB"]; ok {
			if f, err := strconv.ParseFloat(v, 64); err == nil && f >= 0 {
				return f
			}
		}
	}
	return defaultVPUsPerGB
}

// ForKey retrieves costing metadata for a key.
func (rcs *RateCardStore) ForKey(key models.Key, defaultPricing DefaultPricing) (*models.Node, models.PricingMetadata, error) {
	features := strings.Split(key.Features(), ",")
	shape := features[0]
	cpuPriceMultiplier := 1.0
	product := instanceProducts.get(shape)
	if baseShape, multiplier, ok := normalizeOCIInstanceShape(shape); ok {
		baseProduct := instanceProducts.get(baseShape)
		if !baseProduct.isEmpty() {
			shape = baseShape
			cpuPriceMultiplier = multiplier
			product = baseProduct
		}
	}
	var node *models.Node
	// Use the default pricing if the instance product is unknown
	if product.isEmpty() {
		totalCost, err := defaultPricing.TotalInstanceCost()
		if err != nil {
			return nil, models.PricingMetadata{}, fmt.Errorf("failed to parse default Oracle pricing: %w", err)
		}
		log.DedupedWarningf(5, "No OCI rate card for shape %q, using default pricing", shape)
		vcpuCost := defaultPricing.OCPU
		if perOCPU := vcpusPerOCPU(shape, features); perOCPU != 1 {
			vcpuFloat, err := strconv.ParseFloat(vcpuCost, 64)
			if err != nil {
				return nil, models.PricingMetadata{}, err
			}
			vcpuFloat /= perOCPU
			vcpuCost = fmt.Sprintf("%f", vcpuFloat)
		}
		node = &models.Node{
			Cost:     fmt.Sprintf("%f", totalCost),
			VCPUCost: vcpuCost,
			RAM:      defaultPricing.Memory,
			GPU:      defaultPricing.GPU,
		}
	} else {
		ocpuPrice := rcs.prices[product.OCPU].UnitPrice * cpuPriceMultiplier
		ocpuPrice /= vcpusPerOCPU(shape, features)
		memoryPrice := rcs.prices[product.Memory].UnitPrice
		gpuPrice := rcs.prices[product.GPU].UnitPrice
		diskPrice := rcs.prices[product.Disk].UnitPrice
		// convert disk price from Tb/hour to Gb/hour
		diskPrice /= 1000
		totalPrice := diskPrice + ocpuPrice + memoryPrice + gpuPrice
		// Add virtual node pricing if it is being used.
		if len(features) > 1 && features[1] == "true" {
			totalPrice += rcs.prices[virualNodePartNumber].UnitPrice
		}
		node = &models.Node{
			Cost:        fmt.Sprintf("%f", totalPrice),
			StorageCost: fmt.Sprintf("%f", diskPrice),
			VCPUCost:    fmt.Sprintf("%f", ocpuPrice),
			RAMCost:     fmt.Sprintf("%f", memoryPrice),
			GPUCost:     fmt.Sprintf("%f", gpuPrice),
		}
	}
	return node, models.PricingMetadata{}, nil
}

func (rcs *RateCardStore) Store() map[string]Price {
	return rcs.prices
}

func (rcs *RateCardStore) Refresh() (map[string]Price, error) {
	if err := rcs.refresh(); err != nil {
		return nil, err
	}
	return rcs.prices, nil
}

// refresh the prices of Price information
func (rcs *RateCardStore) refresh() error {
	rcr, err := rcs.getProductPricing()
	if err != nil {
		return err
	}
	rcs.prices = rcr.toStore()
	return nil
}

func (rcs *RateCardStore) getProductPricing() (*PricingResponse, error) {
	url := fmt.Sprintf("%s?currencyCode=%s", rcs.url, rcs.currencyCode)
	prBytes, err := rcs.loadMetadata(url)
	if err != nil {
		return nil, err
	}
	pr := &PricingResponse{}
	if err := json.Unmarshal(prBytes, pr); err != nil {
		return nil, err
	}
	return pr, nil
}
func (rcs *RateCardStore) loadMetadata(url string) ([]byte, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := rcs.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get pricing metadata data, got code %d", resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

func (rcr *PricingResponse) toStore() map[string]Price {
	store := map[string]Price{}
	for _, item := range rcr.Items {
		store[item.PartNumber] = item.toRateCard()
	}
	return store
}

func (i Item) hasPrice() bool {
	if len(i.CurrencyCodeLocalizations) < 1 {
		return false
	}
	return len(i.CurrencyCodeLocalizations[0].Prices) > 0
}

func (i Item) toRateCard() Price {
	var unitPrice float64
	var model string
	if i.hasPrice() {
		for _, price := range i.CurrencyCodeLocalizations[0].Prices {
			// Some products have range pricing, we'll take the first non-zero pricing in this case.
			if price.Value > 0 {
				unitPrice = price.Value
				model = price.Model
			}
		}
	}
	return Price{
		ProductName: i.DisplayName,
		Metric:      i.MetricName,
		UnitPrice:   unitPrice,
		Model:       model,
	}
}

func (p Product) isEmpty() bool {
	return p.GPU == "" && p.OCPU == "" && p.Memory == "" && p.Disk == ""
}

// vcpusPerOCPU is how many Kubernetes CPUs one OCPU of the shape provides: two on x86 (one core,
// two threads) and on AmpereOne A2 and A4 (two cores), one on Ampere Altra A1 (one core).
// https://docs.oracle.com/en-us/iaas/Content/Compute/References/computeshapes.htm
func vcpusPerOCPU(shape string, features []string) float64 {
	if isARMArch(features) && strings.Contains(shape, ".A1.") {
		return 1
	}
	return 2
}

func isARMArch(features []string) bool {
	if len(features) < 3 {
		return false
	}
	return strings.HasPrefix(strings.ToLower(features[2]), "arm")
}
