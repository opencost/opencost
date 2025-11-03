package scaleway

import (
	stdjson "encoding/json"
	"errors"
	"fmt"
	"io"
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
	productCatalogAPIURL     = "https://api.scaleway.com/product-catalog/v2alpha1/public-catalog/products?page_size=10000"
	ProductCatalogAPIPricing = "Product Catalog API Pricing"
)

var scalewayHTTPClient = &http.Client{
	Timeout: 30 * time.Second,
}

type ScalewayPricing struct {
	NodesInfos map[string]*ScalewayNode
	PVCost     float64
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
		if zone == "" {
			continue
		}

		switch {
		case product.isInstanceProduct():
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

func ensureScalewayPricingForZone(pricing map[string]*ScalewayPricing, zone string) *ScalewayPricing {
	if pricing == nil {
		pricing = make(map[string]*ScalewayPricing)
	}

	zonePricing, ok := pricing[zone]
	if !ok {
		zonePricing = &ScalewayPricing{
			NodesInfos: make(map[string]*ScalewayNode),
		}
		pricing[zone] = zonePricing
	} else if zonePricing.NodesInfos == nil {
		zonePricing.NodesInfos = make(map[string]*ScalewayNode)
	}

	return zonePricing
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

	pricing, ok := c.Pricing[zone]
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

	return &models.Node{
		Cost:         fmt.Sprintf("%f", info.HourlyPrice),
		PricingType:  models.DefaultPrices,
		VCPU:         vcpu,
		RAM:          ram,
		RAMBytes:     ram,
		Storage:      storage,
		GPU:          gpu,
		InstanceType: instanceType,
		Region:       zone,
		GPUName:      key.GPUType(),
	}, meta, nil
}

func (c *Scaleway) LoadBalancerPricing() (*models.LoadBalancer, error) {
	// Different LB types, lets take the cheaper for now, we can't get the type
	// without a service specifying the type in the annotations
	return &models.LoadBalancer{
		Cost: 0.014,
	}, nil
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
}

func (key *scalewayPVKey) ID() string {
	return ""
}

func (key *scalewayPVKey) GetStorageClass() string {
	return key.StorageClassName
}

func (key *scalewayPVKey) Features() string {
	// Only 1 type of PV for now
	return key.Zone
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
	return &scalewayPVKey{
		Labels:                 pv.Labels,
		StorageClassName:       pv.Spec.StorageClassName,
		StorageClassParameters: parameters,
		Name:                   pv.Name,
		Zone:                   zone,
	}
}

func (c *Scaleway) GpuPricing(nodeLabels map[string]string) (string, error) {
	return "", nil
}

func (c *Scaleway) PVPricing(pvk models.PVKey) (*models.PV, error) {
	c.DownloadPricingDataLock.RLock()
	defer c.DownloadPricingDataLock.RUnlock()

	pricing, ok := c.Pricing[pvk.Features()]
	if !ok {
		log.Debugf("Persistent Volume pricing not found for %s: %s", pvk.GetStorageClass(), pvk.Features())
		return &models.PV{}, nil
	}
	return &models.PV{
		Cost:  fmt.Sprintf("%f", pricing.PVCost),
		Class: pvk.GetStorageClass(),
	}, nil
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
