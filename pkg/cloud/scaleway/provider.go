package scaleway

import (
	stdjson "encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	coreenv "github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/cloud/utils"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/pkg/env"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/scaleway/scaleway-sdk-go/scw"
)

const (
	productCatalogAPIURL         = "https://api.scaleway.com/product-catalog/v2alpha1/public-catalog/products?page_size=10000"
	ProductCatalogAPIPricing     = "Product Catalog API Pricing"
	globalPricingKey             = "__global__"
	defaultLoadBalancerNodeClass = "GP-S"
	defaultLoadBalancerFallback  = 0.02
)

var scalewayHTTPClient = &http.Client{
	Timeout: 30 * time.Second,
}

type ScalewayPricing struct {
	Zone                  string
	NodesInfos            map[string]*ScalewayNode
	PVCost                float64
	LoadBalancerNodeCosts map[string]float64
	LoadBalancerIPCost    float64
}

type ScalewayNode struct {
	HourlyPrice       float64
	VCPUCount         int64
	RAMBytes          int64
	LocalStorageBytes int64
	GPUCount          int64
}

type scalewayProductCatalog struct {
	Products []*scalewayProduct `json:"products"`
}

type scalewayProduct struct {
	SKU             string                 `json:"sku"`
	ServiceCategory string                 `json:"service_category"`
	ProductCategory string                 `json:"product_category"`
	Product         string                 `json:"product"`
	Variant         string                 `json:"variant"`
	Description     string                 `json:"description"`
	Locality        *scalewayLocality      `json:"locality"`
	Price           *scalewayPrice         `json:"price"`
	UnitOfMeasure   *scalewayUnitOfMeasure `json:"unit_of_measure"`
	Properties      *scalewayProperties    `json:"properties"`
	Status          string                 `json:"status"`
}

type scalewayLocality struct {
	Zone   string `json:"zone"`
	Region string `json:"region"`
	Global bool   `json:"global"`
}

type scalewayPrice struct {
	RetailPrice *scalewayRetailPrice `json:"retail_price"`
}

type scalewayRetailPrice struct {
	CurrencyCode string `json:"currency_code"`
	Units        int64  `json:"units"`
	Nanos        int64  `json:"nanos"`
}

type scalewayUnitOfMeasure struct {
	Unit string `json:"unit"`
	Size int64  `json:"size"`
}

type scalewayProperties struct {
	Hardware     *scalewayHardware            `json:"hardware"`
	Instance     *scalewayInstanceProperties  `json:"instance"`
	BlockStorage *scalewayBlockStorageDetails `json:"block_storage"`
}

type scalewayHardware struct {
	CPU     *scalewayHardwareCPU     `json:"cpu"`
	RAM     *scalewayHardwareRAM     `json:"ram"`
	Storage *scalewayHardwareStorage `json:"storage"`
	GPU     *scalewayHardwareGPU     `json:"gpu"`
}

type scalewayHardwareCPU struct {
	Description string                      `json:"description"`
	Arch        string                      `json:"arch"`
	Type        string                      `json:"type"`
	Virtual     *scalewayHardwareCPUVirtual `json:"virtual"`
	Threads     int64                       `json:"threads"`
}

type scalewayHardwareCPUVirtual struct {
	Count int64 `json:"count"`
}

type scalewayHardwareRAM struct {
	Description string `json:"description"`
	Size        int64  `json:"size"`
	Type        string `json:"type"`
}

type scalewayHardwareStorage struct {
	Description string `json:"description"`
	Total       int64  `json:"total"`
}

type scalewayHardwareGPU struct {
	Description string `json:"description"`
	Count       int64  `json:"count"`
	Type        string `json:"type"`
}

type scalewayInstanceProperties struct {
	Range                        string   `json:"range"`
	OfferID                      string   `json:"offer_id"`
	RecommendedReplacementOffers []string `json:"recommended_replacement_offer_ids"`
}

type scalewayBlockStorageDetails struct {
	MinVolumeSize int64 `json:"min_volume_size"`
	MaxVolumeSize int64 `json:"max_volume_size"`
}

type Scaleway struct {
	Clientset               clustercache.ClusterCache
	Config                  models.ProviderConfig
	Pricing                 map[string]*ScalewayPricing
	ClusterRegion           string
	ClusterAccountID        string
	DownloadPricingDataLock sync.RWMutex
}

// PricingSourceSummary returns the pricing source summary for the provider.
// The summary represents what was _parsed_ from the pricing source, not
// everything that was _available_ in the pricing source.
func (c *Scaleway) PricingSourceSummary() interface{} {
	return c.Pricing
}
func (c *Scaleway) DownloadPricingData() error {
	c.DownloadPricingDataLock.Lock()
	defer c.DownloadPricingDataLock.Unlock()

	if len(c.Pricing) != 0 {
		// Already initialized
		return nil
	}

	products, err := fetchScalewayProductCatalog()
	if err != nil {
		return err
	}

	pricingByZone := make(map[string]*ScalewayPricing)

	for _, product := range products {
		if product == nil {
			continue
		}

		zone := product.zone()

		switch {
		case product.isInstanceProduct():
			if zone == "" {
				continue
			}
			instanceType := product.instanceType()
			if instanceType == "" {
				log.Debugf("Scaleway product %s missing instance identifier", product.SKU)
				continue
			}

			nodeInfo, err := product.toScalewayNode()
			if err != nil {
				log.Debugf("Skipping Scaleway product %s: %v", product.SKU, err)
				continue
			}

			zonePricing := ensureScalewayPricingForZone(pricingByZone, zone)
			zonePricing.NodesInfos[instanceType] = nodeInfo

		case product.isBlockStorageProduct():
			price := product.priceValue()
			if price == 0 {
				continue
			}

			zonePricing := ensureScalewayPricingForZone(pricingByZone, zone)
			if product.prefersOverwritePVCost() || zonePricing.PVCost == 0 {
				zonePricing.PVCost = price
			}
		case product.isLoadBalancerNodeProduct():
			if zone == "" {
				continue
			}

			lbClass := product.loadBalancerNodeClass()
			if lbClass == "" {
				continue
			}

			price := product.priceValue()
			if price == 0 {
				continue
			}

			zonePricing := ensureScalewayPricingForZone(pricingByZone, zone)
			if zonePricing.LoadBalancerNodeCosts == nil {
				zonePricing.LoadBalancerNodeCosts = make(map[string]float64)
			}
			zonePricing.LoadBalancerNodeCosts[lbClass] = price
		case product.isLoadBalancerIPProduct():
			if zone == "" {
				continue
			}

			price := product.priceValue()
			if price == 0 {
				continue
			}

			zonePricing := ensureScalewayPricingForZone(pricingByZone, zone)
			zonePricing.LoadBalancerIPCost = price
		}
	}

	c.Pricing = pricingByZone

	return nil
}

func fetchScalewayProductCatalog() ([]*scalewayProduct, error) {
	req, err := http.NewRequest(http.MethodGet, productCatalogAPIURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating Scaleway product catalog request: %w", err)
	}

	resp, err := scalewayHTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("requesting Scaleway product catalog: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("scaleway product catalog request failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var catalog scalewayProductCatalog
	if err := stdjson.NewDecoder(resp.Body).Decode(&catalog); err != nil {
		return nil, fmt.Errorf("decoding Scaleway product catalog: %w", err)
	}

	return catalog.Products, nil
}

func pricingZoneKey(zone string) string {
	key := strings.ToLower(strings.TrimSpace(zone))
	if key == "" {
		return globalPricingKey
	}
	return key
}

func ensureScalewayPricingForZone(pricing map[string]*ScalewayPricing, zone string) *ScalewayPricing {
	if pricing == nil {
		return nil
	}

	key := pricingZoneKey(zone)

	zonePricing, ok := pricing[key]
	if !ok {
		zonePricing = &ScalewayPricing{
			Zone:                  strings.TrimSpace(zone),
			NodesInfos:            make(map[string]*ScalewayNode),
			LoadBalancerNodeCosts: make(map[string]float64),
		}
		pricing[key] = zonePricing
		return zonePricing
	}

	if zonePricing.NodesInfos == nil {
		zonePricing.NodesInfos = make(map[string]*ScalewayNode)
	}
	if zonePricing.LoadBalancerNodeCosts == nil {
		zonePricing.LoadBalancerNodeCosts = make(map[string]float64)
	}
	if zonePricing.Zone == "" && zone != "" {
		zonePricing.Zone = strings.TrimSpace(zone)
	}

	return zonePricing
}

func (c *Scaleway) lookupZonePricing(zone, region string) (*ScalewayPricing, string) {
	if len(c.Pricing) == 0 {
		return nil, ""
	}

	var candidates []string

	if strings.TrimSpace(zone) != "" {
		candidates = append(candidates, zone)
	}

	if strings.TrimSpace(region) != "" {
		candidates = append(candidates, region)
		candidates = append(candidates, c.zoneKeysWithPrefix(region)...)
	}

	if strings.TrimSpace(c.ClusterRegion) != "" {
		candidates = append(candidates, c.ClusterRegion)
		candidates = append(candidates, c.zoneKeysWithPrefix(c.ClusterRegion)...)
	}

	// Fallback to any zone we already know about.
	for key := range c.Pricing {
		candidates = append(candidates, key)
	}

	// Ensure global fallback is last.
	candidates = append(candidates, globalPricingKey)

	seen := make(map[string]struct{}, len(candidates))

	for _, candidate := range candidates {
		key := pricingZoneKey(candidate)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}

		if pricing, ok := c.Pricing[key]; ok && pricing != nil {
			return pricing, key
		}
	}

	return nil, ""
}

func (c *Scaleway) zoneKeysWithPrefix(prefix string) []string {
	normPrefix := strings.ToLower(strings.TrimSpace(prefix))
	if normPrefix == "" || len(c.Pricing) == 0 {
		return nil
	}

	keys := make([]string, 0)
	for key := range c.Pricing {
		if strings.HasPrefix(key, normPrefix) {
			keys = append(keys, key)
		}
	}
	return keys
}

func (p *scalewayProduct) zone() string {
	if p == nil || p.Locality == nil {
		return ""
	}
	return p.Locality.Zone
}

func (p *scalewayProduct) isInstanceProduct() bool {
	if p == nil {
		return false
	}
	if !strings.EqualFold(p.ServiceCategory, "Compute") {
		return false
	}
	if !strings.EqualFold(p.ProductCategory, "Instance") {
		return false
	}
	return p.Properties != nil && p.Properties.Instance != nil
}

func (p *scalewayProduct) instanceType() string {
	if p == nil {
		return ""
	}

	if p.Properties != nil && p.Properties.Instance != nil && p.Properties.Instance.OfferID != "" {
		return p.Properties.Instance.OfferID
	}

	productName := strings.TrimSpace(p.Product)
	if productName == "" {
		return ""
	}

	fields := strings.Fields(productName)
	if len(fields) == 0 {
		return ""
	}

	return strings.TrimSuffix(fields[0], ",")
}

func (p *scalewayProduct) toScalewayNode() (*ScalewayNode, error) {
	if p == nil {
		return nil, errors.New("nil product")
	}

	price := p.priceValue()
	if price == 0 {
		return nil, fmt.Errorf("missing retail price for product %s", p.SKU)
	}

	if p.Properties == nil || p.Properties.Hardware == nil {
		return nil, fmt.Errorf("missing hardware information for product %s", p.SKU)
	}

	hardware := p.Properties.Hardware

	var vcpu int64
	if hardware.CPU != nil {
		if hardware.CPU.Virtual != nil && hardware.CPU.Virtual.Count > 0 {
			vcpu = hardware.CPU.Virtual.Count
		} else if hardware.CPU.Threads > 0 {
			vcpu = hardware.CPU.Threads
		}
	}

	var ramBytes int64
	if hardware.RAM != nil {
		ramBytes = hardware.RAM.Size
	}

	var storageBytes int64
	if hardware.Storage != nil {
		storageBytes = hardware.Storage.Total
	}

	var gpuCount int64
	if hardware.GPU != nil {
		gpuCount = hardware.GPU.Count
	}

	return &ScalewayNode{
		HourlyPrice:       price,
		VCPUCount:         vcpu,
		RAMBytes:          ramBytes,
		LocalStorageBytes: storageBytes,
		GPUCount:          gpuCount,
	}, nil
}

func (p *scalewayProduct) isLoadBalancerNodeProduct() bool {
	if p == nil {
		return false
	}

	if !strings.EqualFold(p.ServiceCategory, "Network") {
		return false
	}

	if !strings.EqualFold(p.ProductCategory, "Loadbalancer") {
		return false
	}

	return strings.Contains(strings.ToLower(p.Product), "node")
}

func (p *scalewayProduct) isLoadBalancerIPProduct() bool {
	if p == nil {
		return false
	}

	if !strings.EqualFold(p.ServiceCategory, "Network") {
		return false
	}

	if !strings.EqualFold(p.ProductCategory, "Loadbalancer") {
		return false
	}

	return strings.Contains(strings.ToLower(p.Product), "ip")
}

func (p *scalewayProduct) loadBalancerNodeClass() string {
	if p == nil {
		return ""
	}

	fields := strings.Fields(p.Product)
	if len(fields) == 0 {
		return ""
	}

	class := fields[len(fields)-1]
	return strings.ToUpper(strings.TrimSpace(class))
}

func (p *scalewayProduct) isBlockStorageProduct() bool {
	if p == nil {
		return false
	}

	if !strings.EqualFold(p.ServiceCategory, "Storage") {
		return false
	}

	if !strings.EqualFold(p.ProductCategory, "Block Storage") {
		return false
	}

	switch p.Product {
	case "Block Storage Volume SSD", "Block Storage Volume Low Latency":
		return true
	default:
		return false
	}
}

func (p *scalewayProduct) prefersOverwritePVCost() bool {
	if p == nil {
		return false
	}
	return p.Product == "Block Storage Volume SSD"
}

func (p *scalewayProduct) priceValue() float64 {
	if p == nil || p.Price == nil || p.Price.RetailPrice == nil {
		return 0
	}
	return p.Price.RetailPrice.asFloat64()
}

func (rp *scalewayRetailPrice) asFloat64() float64 {
	if rp == nil {
		return 0
	}
	value := float64(rp.Units)
	value += float64(rp.Nanos) / 1e9
	return value
}

func (c *Scaleway) AllNodePricing() (interface{}, error) {
	c.DownloadPricingDataLock.RLock()
	defer c.DownloadPricingDataLock.RUnlock()
	return c.Pricing, nil
}

func (p *ScalewayPricing) loadBalancerCost(preferredClass string) (float64, string) {
	if p == nil {
		return 0, ""
	}

	if preferredClass != "" {
		if cost, ok := p.LoadBalancerNodeCosts[preferredClass]; ok && cost > 0 {
			return cost + p.LoadBalancerIPCost, preferredClass
		}
	}

	if len(p.LoadBalancerNodeCosts) == 0 {
		return p.LoadBalancerIPCost, ""
	}

	min := math.MaxFloat64
	minClass := ""
	for class, cost := range p.LoadBalancerNodeCosts {
		if cost > 0 && cost < min {
			min = cost
			minClass = class
		}
	}

	if min == math.MaxFloat64 {
		return p.LoadBalancerIPCost, ""
	}

	return min + p.LoadBalancerIPCost, minClass
}

type scalewayKey struct {
	Labels map[string]string
}

func (k *scalewayKey) Features() string {
	instanceType, _ := util.GetInstanceType(k.Labels)
	zone, _ := util.GetZone(k.Labels)

	return zone + "," + instanceType
}

func (k *scalewayKey) GPUCount() int {
	return 0
}

func (k *scalewayKey) GPUType() string {
	instanceType, _ := util.GetInstanceType(k.Labels)
	if strings.HasPrefix(instanceType, "RENDER") || strings.HasPrefix(instanceType, "GPU") {
		return instanceType
	}
	return ""
}
func (k *scalewayKey) ID() string {
	return ""
}

func (c *Scaleway) NodePricing(key models.Key) (*models.Node, models.PricingMetadata, error) {
	c.DownloadPricingDataLock.RLock()
	defer c.DownloadPricingDataLock.RUnlock()

	meta := models.PricingMetadata{}

	// There is only the zone and the instance ID in the providerID, hence we must use the features
	features := strings.Split(key.Features(), ",")
	if len(features) < 2 {
		return nil, meta, fmt.Errorf("invalid feature format for Scaleway node pricing: `%s`", key.Features())
	}

	zone := features[0]
	instanceType := features[1]

	zoneKey := pricingZoneKey(zone)
	pricing, ok := c.Pricing[zoneKey]
	if !ok {
		return nil, meta, fmt.Errorf("unable to find node pricing matching the features `%s`", key.Features())
	}

	info, ok := pricing.NodesInfos[instanceType]
	if !ok {
		return nil, meta, fmt.Errorf("unable to find node pricing matching the features `%s`", key.Features())
	}

	vcpu := strconv.FormatInt(info.VCPUCount, 10)
	ram := strconv.FormatInt(info.RAMBytes, 10)
	storage := strconv.FormatInt(info.LocalStorageBytes, 10)
	gpu := strconv.FormatInt(info.GPUCount, 10)

	region := zone
	if pricing.Zone != "" {
		region = pricing.Zone
	}

	log.Debugf("Scaleway: node pricing resolved zone=%s instance=%s cost=%.6f vcpu=%s ramBytes=%s", region, instanceType, info.HourlyPrice, vcpu, ram)

	return &models.Node{
		Cost:         fmt.Sprintf("%f", info.HourlyPrice),
		PricingType:  models.DefaultPrices,
		VCPU:         vcpu,
		RAM:          ram,
		RAMBytes:     ram,
		Storage:      storage,
		GPU:          gpu,
		InstanceType: instanceType,
		Region:       region,
		GPUName:      key.GPUType(),
	}, meta, nil
}

func (c *Scaleway) LoadBalancerPricing() (*models.LoadBalancer, error) {
	cpricing, err := c.Config.GetCustomPricingData()
	if err != nil {
		return nil, err
	}

	if strings.TrimSpace(cpricing.DefaultLBPrice) != "" {
		override, parseErr := strconv.ParseFloat(cpricing.DefaultLBPrice, 64)
		if parseErr == nil {
			return &models.LoadBalancer{
				Cost: override,
			}, nil
		}

		log.Warnf("Scaleway: unable to parse defaultLBPrice %q: %v", cpricing.DefaultLBPrice, parseErr)
	}

	c.DownloadPricingDataLock.RLock()
	defer c.DownloadPricingDataLock.RUnlock()

	cost, class, zone := c.findLoadBalancerCost(defaultLoadBalancerNodeClass)
	if cost == 0 {
		log.Debugf("Scaleway: load balancer pricing not found in catalog, using default fallback %.6f", defaultLoadBalancerFallback)
		cost = defaultLoadBalancerFallback
	} else {
		log.Debugf("Scaleway: load balancer pricing resolved cost=%.6f class=%s zone=%s", cost, class, zone)
	}

	return &models.LoadBalancer{
		Cost: cost,
	}, nil
}

func (c *Scaleway) findLoadBalancerCost(preferredClass string) (float64, string, string) {
	if len(c.Pricing) == 0 {
		return 0, "", ""
	}

	if pricing, key := c.lookupZonePricing("", c.ClusterRegion); pricing != nil {
		if cost, class := pricing.loadBalancerCost(preferredClass); cost > 0 {
			zone := pricing.Zone
			if zone == "" {
				zone = key
			}
			return cost, class, zone
		}
	}

	min := math.MaxFloat64
	bestClass := ""
	bestZone := ""

	for key, zonePricing := range c.Pricing {
		if zonePricing == nil {
			continue
		}

		if cost, class := zonePricing.loadBalancerCost(preferredClass); cost > 0 && cost < min {
			min = cost
			bestClass = class
			zone := zonePricing.Zone
			if zone == "" {
				zone = key
			}
			bestZone = zone
		}
	}

	if min == math.MaxFloat64 {
		return 0, "", ""
	}

	return min, bestClass, bestZone
}

func (c *Scaleway) NetworkPricing() (*models.Network, error) {
	// it's free baby!
	return &models.Network{
		ZoneNetworkEgressCost:     0,
		RegionNetworkEgressCost:   0,
		InternetNetworkEgressCost: 0,
	}, nil
}

func (c *Scaleway) GetKey(l map[string]string, n *clustercache.Node) models.Key {
	return &scalewayKey{
		Labels: l,
	}
}

type scalewayPVKey struct {
	Labels                 map[string]string
	StorageClassName       string
	StorageClassParameters map[string]string
	Name                   string
	Zone                   string
	Region                 string
}

func (key *scalewayPVKey) ID() string {
	return ""
}

func (key *scalewayPVKey) GetStorageClass() string {
	return key.StorageClassName
}

func (key *scalewayPVKey) Features() string {
	// Only 1 type of PV for now
	if strings.TrimSpace(key.Zone) != "" {
		return key.Zone
	}
	return key.Region
}

func (c *Scaleway) GetPVKey(pv *clustercache.PersistentVolume, parameters map[string]string, defaultRegion string) models.PVKey {
	// the csi volume handle is the form <az>/<volume-id>
	zone := ""
	if pv.Spec.CSI != nil {
		zoneVolID := strings.Split(pv.Spec.CSI.VolumeHandle, "/")
		if len(zoneVolID) > 0 {
			zone = zoneVolID[0]
		}
	}
	if zone == "" {
		if z, ok := util.GetZone(pv.Labels); ok {
			zone = z
		}
	}

	region := strings.TrimSpace(defaultRegion)
	if r, ok := util.GetRegion(pv.Labels); ok && strings.TrimSpace(r) != "" {
		region = r
	}

	return &scalewayPVKey{
		Labels:                 pv.Labels,
		StorageClassName:       pv.Spec.StorageClassName,
		StorageClassParameters: parameters,
		Name:                   pv.Name,
		Zone:                   zone,
		Region:                 region,
	}
}

func (c *Scaleway) GpuPricing(nodeLabels map[string]string) (string, error) {
	return "", nil
}

func (c *Scaleway) PVPricing(pvk models.PVKey) (*models.PV, error) {
	c.DownloadPricingDataLock.RLock()
	defer c.DownloadPricingDataLock.RUnlock()

	var zone, region string
	if key, ok := pvk.(*scalewayPVKey); ok && key != nil {
		zone = key.Zone
		region = key.Region
	} else {
		zone = pvk.Features()
	}

	pricing, matchedKey := c.lookupZonePricing(zone, region)
	if pricing == nil || pricing.PVCost == 0 {
		log.Debugf("Persistent Volume pricing not found for %s: zone=%q region=%q", pvk.GetStorageClass(), zone, region)
		return &models.PV{}, nil
	}

	result := &models.PV{
		Cost:  fmt.Sprintf("%f", pricing.PVCost),
		Class: pvk.GetStorageClass(),
	}

	if key, ok := pvk.(*scalewayPVKey); ok && key != nil && len(key.StorageClassParameters) > 0 {
		result.Parameters = key.StorageClassParameters
	}

	if matchedKey != "" && matchedKey != globalPricingKey {
		result.Region = pricing.Zone
		if result.Region == "" {
			result.Region = matchedKey
		}
	}

	log.Debugf("Scaleway: PV pricing resolved class=%s zone=%s region=%s matchedKey=%s cost=%s", result.Class, zone, region, matchedKey, result.Cost)

	return result, nil
}

func (c *Scaleway) ServiceAccountStatus() *models.ServiceAccountStatus {
	return &models.ServiceAccountStatus{
		Checks: []*models.ServiceAccountCheck{},
	}
}

func (*Scaleway) ClusterManagementPricing() (string, float64, error) {
	return "", 0.0, nil
}

func (c *Scaleway) CombinedDiscountForNode(instanceType string, isPreemptible bool, defaultDiscount, negotiatedDiscount float64) float64 {
	return 1.0 - ((1.0 - defaultDiscount) * (1.0 - negotiatedDiscount))
}

func (c *Scaleway) Regions() []string {

	regionOverrides := env.GetRegionOverrideList()

	if len(regionOverrides) > 0 {
		log.Debugf("Overriding Scaleway regions with configured region list: %+v", regionOverrides)
		return regionOverrides
	}

	// These are zones but hey, its 2022
	zones := []string{}
	for _, zone := range scw.AllZones {
		zones = append(zones, zone.String())
	}
	return zones
}

func (*Scaleway) ApplyReservedInstancePricing(map[string]*models.Node) {}

func (*Scaleway) GetAddresses() ([]byte, error) {
	return nil, nil
}

func (*Scaleway) GetDisks() ([]byte, error) {
	return nil, nil
}

func (*Scaleway) GetOrphanedResources() ([]models.OrphanedResource, error) {
	return nil, errors.New("not implemented")
}

func (scw *Scaleway) ClusterInfo() (map[string]string, error) {
	remoteEnabled := env.IsRemoteEnabled()

	m := make(map[string]string)
	m["name"] = "Scaleway Cluster #1"
	c, err := scw.GetConfig()
	if err != nil {
		return nil, err
	}
	if c.ClusterName != "" {
		m["name"] = c.ClusterName
	}
	m["provider"] = opencost.ScalewayProvider
	m["region"] = scw.ClusterRegion
	m["account"] = scw.ClusterAccountID
	m["remoteReadEnabled"] = strconv.FormatBool(remoteEnabled)
	m["id"] = coreenv.GetClusterID()
	return m, nil

}

func (c *Scaleway) UpdateConfigFromConfigMap(a map[string]string) (*models.CustomPricing, error) {
	return c.Config.UpdateFromMap(a)
}

func (c *Scaleway) UpdateConfig(r io.Reader, updateType string) (*models.CustomPricing, error) {
	defer c.DownloadPricingData()

	return c.Config.Update(func(c *models.CustomPricing) error {
		a := make(map[string]interface{})
		err := json.NewDecoder(r).Decode(&a)
		if err != nil {
			return err
		}
		for k, v := range a {
			kUpper := utils.ToTitle.String(k) // Just so we consistently supply / receive the same values, uppercase the first letter.
			vstr, ok := v.(string)
			if ok {
				err := models.SetCustomPricingField(c, kUpper, vstr)
				if err != nil {
					return fmt.Errorf("error setting custom pricing field: %w", err)
				}
			} else {
				return fmt.Errorf("type error while updating config for %s", kUpper)
			}
		}

		if env.IsRemoteEnabled() {
			err := utils.UpdateClusterMeta(coreenv.GetClusterID(), c.ClusterName)
			if err != nil {
				return err
			}
		}

		return nil
	})
}
func (scw *Scaleway) GetConfig() (*models.CustomPricing, error) {
	c, err := scw.Config.GetCustomPricingData()
	if err != nil {
		return nil, err
	}
	if c.Discount == "" {
		c.Discount = "0%"
	}
	if c.NegotiatedDiscount == "" {
		c.NegotiatedDiscount = "0%"
	}
	if c.CurrencyCode == "" {
		c.CurrencyCode = "EUR"
	}
	return c, nil
}

func (scw *Scaleway) GetManagementPlatform() (string, error) {
	nodes := scw.Clientset.GetAllNodes()

	if len(nodes) > 0 {
		n := nodes[0]
		if _, ok := n.Labels["k8s.scaleway.com/kapsule"]; ok {
			return "kapsule", nil
		}
		if _, ok := n.Labels["kops.k8s.io/instancegroup"]; ok {
			return "kops", nil
		}
	}
	return "", nil
}

func (c *Scaleway) PricingSourceStatus() map[string]*models.PricingSource {
	c.DownloadPricingDataLock.RLock()
	defer c.DownloadPricingDataLock.RUnlock()

	available := len(c.Pricing) > 0

	return map[string]*models.PricingSource{
		ProductCatalogAPIPricing: {
			Name:      ProductCatalogAPIPricing,
			Enabled:   true,
			Available: available,
		},
	}
}
