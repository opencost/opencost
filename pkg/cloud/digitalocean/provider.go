package digitalocean

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/env"
)

const fallbackPVHourlyRate = 0.00015

type DOKS struct {
	PricingURL            string
	Cache                 *PricingCache
	Products              map[string][]DOProduct
	Config                models.ProviderConfig
	Clientset             clustercache.ClusterCache
	ClusterManagementCost float64
}

type PricingCache struct {
	data       *DOResponse
	lastUpdate time.Time
	mu         sync.Mutex
}

type DOResponse struct {
	Products []DOProduct `json:"products"`
}

type DOProduct struct {
	SKU         string        `json:"sku"`
	ItemType    string        `json:"itemType"`
	DisplayName string        `json:"displayName"`
	Category    string        `json:"category"`
	Prices      []DOPrice     `json:"prices"`
	Allowances  []DOAllowance `json:"allowances,omitempty"`
	Attributes  []DOAttribute `json:"attributes,omitempty"`
	EffectiveAt string        `json:"effectiveAt"`
}

type DOPrice struct {
	Unit      string `json:"unit"`
	Rate      string `json:"rate"`
	MinAmount string `json:"minAmount"`
	MaxAmount string `json:"maxAmount"`
	MinUsage  string `json:"minUsage"`
	MaxUsage  string `json:"maxUsage"`
	Currency  string `json:"currency"`
	Region    string `json:"region"`
}

type DOAllowance struct {
	Quantity    string `json:"quantity"`
	Unit        string `json:"unit"`
	AllowanceId string `json:"allowanceId"`
	Schedule    string `json:"schedule"`
}

type DOAttribute struct {
	Name  string `json:"name"`
	Value string `json:"value"`
	Unit  string `json:"unit"`
}

func NewDOKSProvider(pricingURL string) *DOKS {
	return &DOKS{
		PricingURL: pricingURL,
		Cache:      &PricingCache{},
		Products:   make(map[string][]DOProduct),
	}
}

func NewPricingCache() *PricingCache {
	return &PricingCache{
		data:       nil,
		lastUpdate: time.Time{},
	}
}

func (do *DOKS) fetchPricingData() (*DOResponse, error) {
	do.Cache.mu.Lock()
	defer do.Cache.mu.Unlock()

	// Return cached data if still valid
	if do.Cache.data != nil && time.Since(do.Cache.lastUpdate) < time.Hour {
		log.Debugf("Using cached pricing data (last updated: %v)", do.Cache.lastUpdate)
		return do.Cache.data, nil
	}

	pricingURL := do.PricingURL
	if pricingURL == "" {
		pricingURL = env.GetDOKSPricingURL()
	}
	log.Infof("Fetching DigitalOcean pricing from: %s", pricingURL)

	resp, err := http.Get(pricingURL)
	if err != nil {
		log.Warnf("Failed to fetch pricing from DigitalOcean: %v", err)
		return nil, fmt.Errorf("pricing API fetch error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Warnf("Pricing API returned unexpected status: %d", resp.StatusCode)
		return nil, fmt.Errorf("pricing API returned status: %d", resp.StatusCode)
	}

	var data DOResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		log.Errorf("Failed to decode pricing JSON: %v", err)
		return nil, fmt.Errorf("failed to decode pricing response: %w", err)
	}

	// Categorize products by item type
	categorized := make(map[string][]DOProduct)
	for _, product := range data.Products {
		log.Debugf("Indexing product: SKU=%s, ItemType=%s, Name=%s", product.SKU, product.ItemType, product.DisplayName)
		categorized[product.ItemType] = append(categorized[product.ItemType], product)
	}

	// Cache and return
	do.Products = categorized
	do.Cache.data = &data
	do.Cache.lastUpdate = time.Now()

	log.Infof("Successfully updated DigitalOcean pricing cache (%d products)", len(data.Products))
	return do.Cache.data, nil
}

// DO Node
type doksKey struct {
	Labels     map[string]string
	ProviderID string
}

func (do *DOKS) GetKey(labels map[string]string, n *clustercache.Node) models.Key {
	var providerID string
	if n != nil {
		providerID = n.SpecProviderID
		if providerID != "" {
			labels["providerID"] = providerID
		}

		cpuQty := n.Status.Capacity["cpu"]
		cpuCores := cpuQty.MilliValue() / 1000
		labels["node.opencost.io/cpu"] = fmt.Sprintf("%d", cpuCores)
		log.Debugf("Set label 'node.opencost.io/cpu' = %d", cpuCores)

		memQty := n.Status.Capacity["memory"]
		memGiB := int(math.Ceil(float64(memQty.Value()) / (1024 * 1024 * 1024)))
		labels["node.opencost.io/ram"] = fmt.Sprintf("%d", memGiB)
		log.Debugf("Set label 'node.opencost.io/ram' = %d", memGiB)

	}

	return &doksKey{
		Labels:     labels,
		ProviderID: providerID,
	}
}

func (k *doksKey) ID() string {
	if it, ok := k.Labels["node.kubernetes.io/instance-type"]; ok {
		return it
	}
	if it, ok := k.Labels["beta.kubernetes.io/instance-type"]; ok {
		return it
	}
	log.Debugf("doksKey: missing instance-type. Labels: %+v", k.Labels)
	return ""
}

func (k *doksKey) Features() string {
	features := map[string]string{}

	for _, label := range []string{
		"node.kubernetes.io/instance-type",
		"beta.kubernetes.io/instance-type",
		"kubernetes.io/arch",
		"beta.kubernetes.io/arch",
		"node.opencost.io/ram",
		"node.opencost.io/cpu",
	} {
		if val, ok := k.Labels[label]; ok {
			features[label] = val
		}
	}

	var parts []string
	for k, v := range features {
		parts = append(parts, fmt.Sprintf("%s=%s", k, v))
	}

	sort.Strings(parts)
	return strings.Join(parts, ",")
}

func (k *doksKey) GPUType() string {
	return ""
}

func (k *doksKey) String() string {
	if instanceType, ok := k.Labels["node.kubernetes.io/instance-type"]; ok {
		return instanceType
	}
	if instanceType, ok := k.Labels["beta.kubernetes.io/instance-type"]; ok {
		return instanceType
	}
	return ""
}

func (k *doksKey) GPUCount() int {
	return 0
}

type SlugBase struct {
	BaseSlug   string
	BaseCost   float64
	BaseVCPU   int
	BaseRAMGiB int
}

type slugSeeds struct {
	BaseVCPU    int
	BaseHourly  float64
	RamPerVCPU  int
	IntelHourly float64
}

var slugFamilySeed = map[string]slugSeeds{
	"c":     {BaseVCPU: 4, BaseHourly: 0.12500, RamPerVCPU: 2, IntelHourly: 0.16220},
	"c2":    {BaseVCPU: 4, BaseHourly: 0.13988, RamPerVCPU: 2, IntelHourly: 0.18155},
	"g":     {BaseVCPU: 4, BaseHourly: 0.18750, RamPerVCPU: 4, IntelHourly: 0.22470},
	"gd":    {BaseVCPU: 4, BaseHourly: 0.20238, RamPerVCPU: 4, IntelHourly: 0.23512},
	"m":     {BaseVCPU: 8, BaseHourly: 0.50000, RamPerVCPU: 8, IntelHourly: 0.58929},
	"m3":    {BaseVCPU: 8, BaseHourly: 0.61905, RamPerVCPU: 8, IntelHourly: 0.65476},
	"m6":    {BaseVCPU: 8, BaseHourly: 0.77976, RamPerVCPU: 8, IntelHourly: 0},
	"s":     {BaseVCPU: 4, BaseHourly: 0.07143, RamPerVCPU: 2, IntelHourly: 0.08333},
	"so":    {BaseVCPU: 8, BaseHourly: 0.77976, RamPerVCPU: 8, IntelHourly: 0.77976},
	"so1_5": {BaseVCPU: 8, BaseHourly: 0.97024, RamPerVCPU: 8, IntelHourly: 0.82738},
}

// TODO Refine GPU pricing and move to GPU method once GPUs are fully GA
var gpuHourly = map[string]float64{
	"gpu-4000adax1-20gb": 0.76,
	"gpu-6000adax1-48gb": 1.57,
	"gpu-h100x1-80gb":    3.39,
	"gpu-h100x8-640gb":   23.92,
	"gpu-h200x1-141gb":   3.44,
	"gpu-h200x8-1128gb":  27.52,
	"gpu-l40sx1-48gb":    1.57,
	"gpu-mi300x1-192gb":  1.99,
	"gpu-mi300x8-1536gb": 15.92,
}

var (
	reVCpu        = regexp.MustCompile(`(\d+)\s*vcpu`)
	reRAM         = regexp.MustCompile(`(\d+)\s*gb`)
	reSimpleCount = regexp.MustCompile(`^[a-z0-9_]+-(\d+)(?:-|$)`)
)

func extractResources(slug string) (int, int, bool) {
	parts := strings.Split(slug, "-")

	var vcpu, ram int
	var foundVCPU, foundRAM bool

	for _, part := range parts {
		switch {
		case strings.HasSuffix(part, "vcpu"):
			v, err := strconv.Atoi(strings.TrimSuffix(part, "vcpu"))
			if err == nil {
				vcpu = v
				foundVCPU = true
			}
		case strings.HasSuffix(part, "gb"):
			v, err := strconv.Atoi(strings.TrimSuffix(part, "gb"))
			if err == nil {
				ram = v
				foundRAM = true
			}
		default:
			// Fallback case for just "8", "16", etc.
			v, err := strconv.Atoi(part)
			if err == nil {
				if !foundVCPU {
					vcpu = v
					foundVCPU = true
				} else if !foundRAM {
					ram = v
					foundRAM = true
				}
			}
		}
	}

	// If vCPU found but not RAM, assume RAM is 2x vCPU, works for all c families
	if foundVCPU && !foundRAM {
		ram = 2 * vcpu
		foundRAM = true
	}

	return vcpu, ram, foundVCPU && foundRAM
}

// Estimate cost based on slug pattern and scale from base slugs which are seeded
func estimateCostFromSlug(slug string) (float64, int, int, bool) {
	s := strings.ToLower(strings.TrimSpace(slug))

	// GPUs are to be handled as a separate case
	if strings.HasPrefix(s, "gpu-") {
		if h, ok := gpuHourly[s]; ok {
			vcpu, ram := extractVCpuRAMGuess(s, "", 0) // we don’t rely on these for pricing
			return h, vcpu, ram, true
		}
		return 0, 0, 0, false
	}

	dashPosition := strings.IndexByte(s, '-')
	if dashPosition <= 0 {
		return 0, 0, 0, false
	}
	family := s[:dashPosition]
	seed, ok := slugFamilySeed[family]
	if !ok {
		return 0, 0, 0, false
	}

	hasIntel := strings.Contains(s, "-intel")

	vcpu, ramGiB := extractVCpuRAMGuess(s, family, seed.RamPerVCPU)
	if vcpu == 0 {
		return 0, 0, 0, false
	}
	if ramGiB == 0 && seed.RamPerVCPU > 0 {
		ramGiB = seed.RamPerVCPU * vcpu
	}
	scale := float64(vcpu) / float64(seed.BaseVCPU)
	hourly := seed.BaseHourly * scale

	if hasIntel && seed.IntelHourly > 0 && seed.BaseHourly > 0 {
		mult := seed.IntelHourly / seed.BaseHourly
		hourly *= mult
	}

	return hourly, vcpu, ramGiB, true
}

// TODO Fix GPU Pricing after GA
func extractVCpuRAMGuess(slugLower, family string, ramPerVCPU int) (vcpu int, ramGiB int) {
	// Regex for matching CPU, we try to find CPU first
	// If RAM not found, we can multiply VCPU by 2 to find it
	if m := reVCpu.FindStringSubmatch(slugLower); len(m) == 2 {
		if n, _ := strconv.Atoi(m[1]); n > 0 {
			vcpu = n
		}
	}
	if m := reRAM.FindStringSubmatch(slugLower); len(m) == 2 {
		if n, _ := strconv.Atoi(m[1]); n > 0 {
			ramGiB = n
		}
	}
	if vcpu == 0 {
		if m := reSimpleCount.FindStringSubmatch(slugLower); len(m) == 2 {
			if n, _ := strconv.Atoi(m[1]); n > 0 {
				vcpu = n
			}
		}
	}

	if ramGiB == 0 && vcpu > 0 && ramPerVCPU > 0 {
		ramGiB = vcpu * ramPerVCPU
	}
	return
}

var (
	vcpuRegex = regexp.MustCompile(`(?i)(\d+)\s*VCPU`)
	ramRegex  = regexp.MustCompile(`(?i)(\d+)\s*GB\s*RAM`)
)

func extractSpecsFromDisplayName(name string) (vcpu int, memoryGiB int, err error) {
	vcpuMatches := vcpuRegex.FindStringSubmatch(name)
	ramMatches := ramRegex.FindStringSubmatch(name)

	if len(vcpuMatches) < 2 || len(ramMatches) < 2 {
		return 0, 0, fmt.Errorf("could not extract specs from displayName: %q", name)
	}

	vcpu, err = strconv.Atoi(vcpuMatches[1])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid vCPU format: %v", err)
	}

	memoryGiB, err = strconv.Atoi(ramMatches[1])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid RAM format: %v", err)
	}

	return vcpu, memoryGiB, nil
}

func parseResources(features string) (int, int, error) {
	parts := strings.Split(features, ",")
	var cpu, ram int
	for _, part := range parts {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			continue
		}
		switch kv[0] {
		case "node.opencost.io/cpu":
			val, err := strconv.Atoi(kv[1])
			if err == nil {
				cpu = val
			}
		case "node.opencost.io/ram":
			val, err := strconv.Atoi(kv[1])
			if err == nil {
				ram = val
			}
		}
	}

	if cpu > 0 && ram > 0 {
		return cpu, ram, nil
	}
	return 0, 0, fmt.Errorf("cpu or ram not found in features")
}

func (do *DOKS) NodePricing(key models.Key) (*models.Node, models.PricingMetadata, error) {
	log.Debugf("Fetching DigitalOcean pricing data (key: %s)", key)

	// Try fetching catalog; fallback is okay
	_, err := do.fetchPricingData()
	if err != nil {
		log.Warnf("Failed to fetch catalog: %v. Will try estimation or fallback.", err)
	}

	arch := parseArch(key.Features())
	slug := key.ID()

	// Try parsing vCPU/RAM from labels
	vcpu, ram, err := parseResources(key.Features())
	if err != nil || vcpu == 0 || ram == 0 {
		log.Infof("Failed to extract CPU/RAM from features. Trying slug: %s", slug)

		var ok bool
		// Try getting from slug (e.g., "s-2vcpu-4gb")
		vcpu, ram, ok = extractResources(slug)
		if !ok {
			// Fallback: RAM = 2x CPU if CPU is known, cases like c-2
			if vcpu > 0 {
				ram = vcpu * 2
				log.Warnf("Only CPU found. Assuming RAM = 2 * CPU → %dGiB", ram)
			} else {
				log.Warnf("Could not extract vCPU/RAM from features or slug. Returning fallback.")
				return fallbackNode(slug)
			}
		}
	}

	// Search for matching product in the DigitalOcean catalog
	for _, products := range do.Products {
		for _, product := range products {
			if product.ItemType != "K8S_WORKER_NODE" {
				continue
			}

			productVCPU, productRAM, err := extractSpecsFromDisplayName(product.DisplayName)
			if err != nil {
				continue
			}

			if productVCPU == vcpu && productRAM == ram {
				node, meta, err := do.productToNode(product, vcpu, ram, arch)
				if err != nil {
					log.Warnf("Failed to convert product %s to node: %v", product.SKU, err)
					continue
				}
				return node, meta, nil
			}
		}
	}

	log.Warnf("No matching product found for slug %s (vCPU: %d, RAM: %d), falling back", slug, vcpu, ram)
	return fallbackNode(slug)
}

func parseArch(features string) string {
	parts := strings.Split(features, ",")
	for _, part := range parts {
		pair := strings.SplitN(part, "=", 2)
		if len(pair) == 2 && (pair[0] == "kubernetes.io/arch" || pair[0] == "beta.kubernetes.io/arch") {
			return pair[1]
		}
	}
	return ""
}

func (do *DOKS) productToNode(product DOProduct, vcpu int, ramGiB int, arch string) (*models.Node, models.PricingMetadata, error) {
	if len(product.Prices) == 0 {
		return nil, models.PricingMetadata{
			Currency: "USD",
			Source:   "digitalocean",
			Warnings: []string{"product has no prices"},
		}, fmt.Errorf("no pricing data for product: %s", product.SKU)
	}

	price := product.Prices[0]
	rate, err := strconv.ParseFloat(price.Rate, 64)
	if err != nil {
		return nil, models.PricingMetadata{
			Currency: "USD",
			Source:   "digitalocean",
			Warnings: []string{"invalid price rate format"},
		}, fmt.Errorf("invalid rate for %s: %v", product.SKU, err)
	}

	var hourlyCost float64
	switch price.Unit {
	case "ITEM_PER_SECOND":
		hourlyCost = rate * 3600
	case "ITEM_PER_HOUR":
		hourlyCost = rate
	default:
		return nil, models.PricingMetadata{
			Currency: "USD",
			Source:   "digitalocean",
			Warnings: []string{"unsupported pricing unit"},
		}, fmt.Errorf("unsupported unit: %s", price.Unit)
	}

	// Assuming CPU and RAM are priced similarly
	totalUnits := float64(vcpu + ramGiB)
	vcpuCost := hourlyCost * float64(vcpu) / totalUnits
	ramCost := hourlyCost * float64(ramGiB) / totalUnits

	if arch == "" {
		arch = "amd64"
	}

	return &models.Node{
			Cost:         fmt.Sprintf("%.5f", hourlyCost),
			VCPUCost:     fmt.Sprintf("%.5f", vcpuCost),
			RAMCost:      fmt.Sprintf("%.5f", ramCost),
			VCPU:         strconv.Itoa(vcpu),
			RAM:          fmt.Sprintf("%dGiB", ramGiB),
			InstanceType: product.DisplayName,
			Region:       price.Region,
			UsageType:    product.ItemType,
			PricingType:  models.DefaultPrices,
			ArchType:     arch,
		}, models.PricingMetadata{
			Currency: "USD",
			Source:   "digitalocean",
		}, nil
}

func fallbackNode(slug string) (*models.Node, models.PricingMetadata, error) {
	if cost, vcpu, ram, ok := estimateCostFromSlug(slug); ok {
		totalUnits := float64(vcpu + ram)
		if totalUnits == 0 {
			return nil, models.PricingMetadata{
				Currency: "USD",
				Source:   "static-fallback",
				Warnings: []string{"invalid vCPU and RAM (0) for fallback"},
			}, fmt.Errorf("invalid fallback spec: totalUnits=0")
		}

		unitCost := cost / totalUnits

		log.Infof("FallbackNode (estimated): %s , hourly=%.5f, vcpuUnit=%.5f, ramUnit=%.5f", slug, cost, unitCost, unitCost)

		return &models.Node{
				Cost:         fmt.Sprintf("%.5f", cost),
				VCPUCost:     fmt.Sprintf("%.5f", unitCost),
				RAMCost:      fmt.Sprintf("%.5f", unitCost),
				VCPU:         strconv.Itoa(vcpu),
				RAM:          fmt.Sprintf("%dGiB", ram),
				InstanceType: slug,
				Region:       "global",
				UsageType:    "static-fallback",
				PricingType:  models.DefaultPrices,
				ArchType:     "amd64",
			}, models.PricingMetadata{
				Currency: "USD",
				Source:   "static-fallback",
				Warnings: []string{"used estimated fallback"},
			}, nil
	}

	return nil, models.PricingMetadata{
		Currency: "USD",
		Source:   "none",
		Warnings: []string{"no fallback available"},
	}, fmt.Errorf("no fallback pricing for slug: %s", slug)
}

type doksPVKey struct {
	id           string
	storageClass string
	sizeBytes    int64
	ProviderID   string
	region       string
}

func (k *doksPVKey) ID() string {
	return k.ProviderID
}

func (k *doksPVKey) SizeGiB() int64 {
	return k.sizeBytes / (1024 * 1024 * 1024)
}

// Features Only one type of PV
func (k *doksPVKey) Features() string {
	return ""
}

func (k *doksPVKey) GetStorageClass() string {
	return k.storageClass
}

func (do *DOKS) PVPricing(key models.PVKey) (*models.PV, error) {
	log.Debug("Fetching DigitalOcean block storage pricing")

	_, err := do.fetchPricingData()
	if err != nil {
		log.Warnf("Failed to fetch PV pricing data: %v, using fallback", err)
		return fallbackPV(key)
	}

	products, ok := do.Products["K8S_VOLUME"]
	if !ok || len(products) == 0 {
		log.Warn("No 'K8S_VOLUME' product found in catalog, using fallback")
		return fallbackPV(key)
	}

	product := products[0]
	if len(product.Prices) == 0 {
		log.Warn("No pricing info found for K8S_VOLUME, using fallback")
		return fallbackPV(key)
	}

	price := product.Prices[0]
	if price.Unit != "GIB_PER_HOUR" {
		log.Warnf("Unsupported PV price unit: %s, expected GIB_PER_HOUR. Using fallback.", price.Unit)
		return fallbackPV(key)
	}

	rate, err := strconv.ParseFloat(price.Rate, 64)
	if err != nil {
		log.Warnf("Failed to parse PV rate: %v, using fallback", err)
		return fallbackPV(key)
	}

	k, ok := key.(*doksPVKey)
	var sizeGB int64
	if ok {
		sizeGB = k.SizeGiB()
	}

	return &models.PV{
		Cost:       fmt.Sprintf("%.5f", rate),
		CostPerIO:  "0",
		Class:      key.GetStorageClass(),
		Size:       fmt.Sprintf("%d", sizeGB),
		Region:     price.Region,
		ProviderID: key.ID(),
		Parameters: nil,
	}, nil
}

func fallbackPV(key models.PVKey) (*models.PV, error) {
	k, ok := key.(*doksPVKey)
	var sizeGB int64
	if ok {
		sizeGB = k.SizeGiB()
	}

	region := "global"
	if ok && k.region != "" {
		region = k.region
	}

	log.Infof("Using fallback PV pricing: %.5f USD/GiB/hr | Class=%s | SizeGiB=%d | Region=%s | ID=%s",
		fallbackPVHourlyRate, key.GetStorageClass(), sizeGB, region, key.ID())

	return &models.PV{
		Cost:       fmt.Sprintf("%.5f", fallbackPVHourlyRate),
		CostPerIO:  "0",
		Class:      key.GetStorageClass(),
		Size:       fmt.Sprintf("%d", sizeGB),
		Region:     region,
		ProviderID: key.ID(),
		Parameters: nil,
	}, nil
}

// LoadBalancerPricing returns the hourly cost of a Load Balancer in DigitalOcean (DOKS).
//
// DigitalOcean offers multiple Load Balancers with different prices:
//
// - Public HTTP Load Balancer:           ~$0.01786/hr
// - Private Network Load Balancer:      ~$0.02232/hr
// - Public Network Load Balancer:       ~$0.02232/hr
// - Statically sized Load Balancers:    $0.01786–$0.10714/hr
//
// However, the current OpenCost provider interface does not pass information about
// individual Load Balancer characteristics (like annotations or network mode).
//
// As a result, this implementation uses a fixed average hourly rate of $0.02,
// which is representative of the most common DO LBs.
//
// TODO Once the provider interface supports more granular Load Balancer metadata,
// this method should be updated to assign costs more precisely.
func (do *DOKS) LoadBalancerPricing() (*models.LoadBalancer, error) {
	hourlyCost := 0.02
	return &models.LoadBalancer{
		Cost: hourlyCost,
	}, nil
}

func (do *DOKS) NetworkPricing() (*models.Network, error) {
	// fallback
	const (
		defaultZoneEgress        = 0.00
		defaultRegionEgress      = 0.00
		defaultInternetEgress    = 0.01
		defaultNatGatewayEgress  = 0.045
		defaultNatGatewayIngress = 0.045
	)

	log.Infof("NetworkPricing: retrieving custom pricing data")
	cpricing, err := do.GetConfig()
	if err != nil || isDefaultNetworkPricing(cpricing) {
		log.Warnf("NetworkPricing: failed to load custom pricing data: %v", err)
		log.Infof("NetworkPricing: using fallback network prices: zone=%.4f, region=%.4f, internet=%.4f",
			defaultZoneEgress, defaultRegionEgress, defaultInternetEgress)
		return &models.Network{
			ZoneNetworkEgressCost:     defaultZoneEgress,
			RegionNetworkEgressCost:   defaultRegionEgress,
			InternetNetworkEgressCost: defaultInternetEgress,
			NatGatewayEgressCost:      defaultNatGatewayEgress,
			NatGatewayIngressCost:     defaultNatGatewayIngress,
		}, nil
	}

	znec := parseWithDefault(cpricing.ZoneNetworkEgress, defaultZoneEgress, "ZoneNetworkEgress")
	rnec := parseWithDefault(cpricing.RegionNetworkEgress, defaultRegionEgress, "RegionNetworkEgress")
	inec := parseWithDefault(cpricing.InternetNetworkEgress, defaultInternetEgress, "InternetNetworkEgress")
	nge := parseWithDefault(cpricing.NatGatewayEgress, defaultNatGatewayEgress, "NatGatewayEgress")
	ngi := parseWithDefault(cpricing.NatGatewayIngress, defaultNatGatewayIngress, "NatGatewayIngress")

	log.Infof("NetworkPricing: using parsed values: zone=%.4f/GiB, region=%.4f/GiB, internet=%.4f/GIB", znec, rnec, inec)

	return &models.Network{
		ZoneNetworkEgressCost:     znec,
		RegionNetworkEgressCost:   rnec,
		InternetNetworkEgressCost: inec,
		NatGatewayEgressCost:      nge,
		NatGatewayIngressCost:     ngi,
	}, nil
}

func parseWithDefault(val string, fallback float64, label string) float64 {
	if val == "" {
		log.Warnf("NetworkPricing: missing value for %s, using fallback %.4f", label, fallback)
		return fallback
	}
	parsed, err := strconv.ParseFloat(val, 64)
	if err != nil {
		log.Warnf("NetworkPricing: failed to parse %s='%s', using fallback %.4f", label, val, fallback)
		return fallback
	}
	return parsed
}

func isDefaultNetworkPricing(cp *models.CustomPricing) bool {
	return cp != nil &&
		cp.ZoneNetworkEgress == "0.01" &&
		cp.RegionNetworkEgress == "0.01" &&
		cp.InternetNetworkEgress == "0.12" &&
		cp.NatGatewayEgress == "0.045" &&
		cp.NatGatewayIngress == "0.045"
}

func (do *DOKS) AllNodePricing() (interface{}, error) {
	_, _ = do.fetchPricingData()
	return do.Cache, nil
}

func (do *DOKS) AllPVPricing() (map[models.PVKey]*models.PV, error) {
	_, err := do.fetchPricingData()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch pricing data: %w", err)
	}

	products, ok := do.Products["K8S_VOLUME"]
	if !ok || len(products) == 0 {
		return nil, fmt.Errorf("no PV products found")
	}

	// Only one PV product
	product := products[0]
	key := &doksPVKey{
		id:           product.SKU,
		storageClass: "do-block-storage",
	}

	pv, err := do.PVPricing(key)
	if err != nil {
		return nil, fmt.Errorf("failed to get PV pricing: %w", err)
	}

	return map[models.PVKey]*models.PV{
		key: pv,
	}, nil
}

func (do *DOKS) GetPVKey(pv *clustercache.PersistentVolume, parameters map[string]string, defaultRegion string) models.PVKey {
	var storageClass string
	if pv.Spec.StorageClassName != "" {
		storageClass = pv.Spec.StorageClassName
	}

	var volumeHandle string
	if pv.Spec.CSI != nil {
		volumeHandle = pv.Spec.CSI.VolumeHandle
	}

	sizeBytes := pv.Spec.Capacity.Storage().Value()

	// Region is in node affinity
	region := defaultRegion
	if pv.Spec.NodeAffinity != nil && pv.Spec.NodeAffinity.Required != nil {
		for _, term := range pv.Spec.NodeAffinity.Required.NodeSelectorTerms {
			for _, expr := range term.MatchExpressions {
				if expr.Key == "region" && len(expr.Values) > 0 {
					region = expr.Values[0]
					break
				}
			}
		}
	}

	return &doksPVKey{
		id:           pv.Name,
		storageClass: storageClass,
		sizeBytes:    sizeBytes,
		ProviderID:   volumeHandle,
		region:       region,
	}
}

func (do *DOKS) ClusterInfo() (map[string]string, error) {
	return map[string]string{"provider": "digitalocean", "platform": "doks"}, nil
}

func (do *DOKS) GetAddresses() ([]byte, error) {
	return nil, nil
}

func (do *DOKS) GetDisks() ([]byte, error) {
	return nil, nil
}

func (do *DOKS) GetOrphanedResources() ([]models.OrphanedResource, error) {
	return nil, nil
}

func (do *DOKS) GpuPricing(input map[string]string) (string, error) {
	return "", nil
}

func (do *DOKS) DownloadPricingData() error {
	_, err := do.fetchPricingData()
	return err
}

func (do *DOKS) UpdateConfig(r io.Reader, updateType string) (*models.CustomPricing, error) {
	return nil, nil
}

func (do *DOKS) UpdateConfigFromConfigMap(map[string]string) (*models.CustomPricing, error) {
	return nil, nil
}

func (do *DOKS) GetConfig() (*models.CustomPricing, error) {
	if do.Config == nil {
		log.Errorf("DOKS: ProviderConfig is nil")
		return nil, fmt.Errorf("provider config not available")
	}

	customPricing, err := do.Config.GetCustomPricingData()
	if err != nil {
		log.Errorf("DOKS: failed to get custom pricing data: %v", err)
		return nil, err
	}
	return customPricing, nil
}

func (do *DOKS) GetManagementPlatform() (string, error) {
	return "DOKS", nil
}

func (do *DOKS) ApplyReservedInstancePricing(map[string]*models.Node) {}

func (do *DOKS) ServiceAccountStatus() *models.ServiceAccountStatus {
	return &models.ServiceAccountStatus{}
}

func (do *DOKS) PricingSourceStatus() map[string]*models.PricingSource {
	return map[string]*models.PricingSource{}
}

func (do *DOKS) ClusterManagementPricing() (string, float64, error) {
	return "", 0, nil
}

func (do *DOKS) CombinedDiscountForNode(string, bool, float64, float64) float64 {
	return 0
}

func (do *DOKS) Regions() []string {
	return []string{"nyc1", "sfo3", "ams3"}
}

func (do *DOKS) PricingSourceSummary() interface{} {
	return nil
}

func (do *DOKS) GetClusterManagementPricing() float64 {
	return do.ClusterManagementCost
}

func (do *DOKS) CustomPricingEnabled() bool {
	return false
}
