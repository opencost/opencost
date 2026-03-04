package ovh

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	coreenv "github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util"
	coreJSON "github.com/opencost/opencost/core/pkg/util/json"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/cloud/utils"
	"github.com/opencost/opencost/pkg/env"
)

const (
	OVHCatalogPricing = "OVH Catalog Pricing"

	BillingLabel  = "ovh.opencost.io/billing"
	NodepoolLabel = "nodepool"

	microcentsPerUnit = 100_000_000.0
	hoursPerMonth     = 730.0
)

// GPU instance prefixes on OVH
var gpuPrefixes = []string{"t2-", "l4-", "l40s-", "a10-", "a100-"}

// OVH MKS regions — update periodically or use REGION_OVERRIDE_LIST env var.
// Source: https://us.ovhcloud.com/public-cloud/regions-availability/
var ovhRegions = []string{
	"BHS5", "DE1", "GRA5", "GRA7", "GRA9", "GRA11",
	"OR1", "SBG5", "SGP1", "SYD1", "UK1", "VA1", "WAW1",
}

// Storage class to OVH volume type mapping
var storageClassToVolumeType = map[string]string{
	"csi-cinder-high-speed-gen2": "high-speed-gen2",
	"csi-cinder-high-speed":      "high-speed",
	"csi-cinder-classic":         "classic",
}

// OVH implements the models.Provider interface for OVHcloud.
type OVH struct {
	Clientset        clustercache.ClusterCache
	Config           models.ProviderConfig
	Pricing          map[string]*OVHFlavorPricing
	VolumePricing    map[string]float64
	ClusterRegion    string
	ClusterAccountID string
	DownloadLock     sync.RWMutex
	catalogURL       string
	monthlyNodepools []string
}

// OVHFlavorPricing holds pricing and specs for an OVH instance flavor.
type OVHFlavorPricing struct {
	HourlyPrice  float64
	MonthlyPrice float64 // monthly price converted to hourly (/730)
	PlanCode     string
	VCPU         int
	RAM          int // GB
	Disk         int // GB
	GPU          int
	GPUName      string
}

// Catalog JSON types

type ovhCatalog struct {
	Plans  []ovhPlan  `json:"plans"`
	Addons []ovhAddon `json:"addons"`
}

type ovhPlan struct {
	PlanCode      string           `json:"planCode"`
	AddonFamilies []ovhAddonFamily `json:"addonFamilies"`
}

type ovhAddonFamily struct {
	Name   string   `json:"name"`
	Addons []string `json:"addons"`
}

type ovhAddon struct {
	PlanCode string       `json:"planCode"`
	Product  string       `json:"product"`
	Pricings []ovhPricing `json:"pricings"`
	Blobs    *ovhBlobs    `json:"blobs"`
}

type ovhPricing struct {
	Price int64  `json:"price"`
	Type  string `json:"type"`
}

type ovhBlobs struct {
	Technical  *ovhTechnical  `json:"technical"`
	Commercial *ovhCommercial `json:"commercial"`
}

type ovhTechnical struct {
	CPU     *ovhCPU     `json:"cpu"`
	Memory  *ovhMemory  `json:"memory"`
	Storage *ovhStorage `json:"storage"`
	GPU     *ovhGPU     `json:"gpu"`
	Name    string      `json:"name"`
}

type ovhCommercial struct {
	BrickSubtype string `json:"brickSubtype"`
}

type ovhCPU struct {
	Cores float64 `json:"cores"`
}

type ovhMemory struct {
	Size float64 `json:"size"`
}

type ovhStorage struct {
	Disks []ovhDisk `json:"disks"`
}

type ovhDisk struct {
	Capacity float64 `json:"capacity"`
}

type ovhGPU struct {
	Number int    `json:"number"`
	Model  string `json:"model"`
}

// parseCatalog extracts instance and volume pricing from the OVH public cloud catalog.
func parseCatalog(data []byte) (map[string]*OVHFlavorPricing, map[string]float64, error) {
	var catalog ovhCatalog
	if err := json.Unmarshal(data, &catalog); err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal OVH catalog: %w", err)
	}

	// Find the project.2018 plan and collect addon planCodes
	instanceAddons := make(map[string]bool)
	volumeAddons := make(map[string]bool)

	var projectPlan *ovhPlan
	for i := range catalog.Plans {
		if catalog.Plans[i].PlanCode == "project.2018" {
			projectPlan = &catalog.Plans[i]
			break
		}
	}
	if projectPlan == nil {
		return nil, nil, fmt.Errorf("project.2018 plan not found in OVH catalog")
	}

	for _, family := range projectPlan.AddonFamilies {
		switch family.Name {
		case "instance":
			for _, a := range family.Addons {
				instanceAddons[a] = true
			}
		case "volume":
			for _, a := range family.Addons {
				volumeAddons[a] = true
			}
		}
	}

	pricing := make(map[string]*OVHFlavorPricing)
	volumePricing := make(map[string]float64)

	for _, addon := range catalog.Addons {
		if instanceAddons[addon.PlanCode] {
			parseInstanceAddon(addon, pricing)
		} else if volumeAddons[addon.PlanCode] {
			parseVolumeAddon(addon, volumePricing)
		}
	}

	return pricing, volumePricing, nil
}

// parseInstanceAddon extracts flavor pricing from an instance addon entry.
func parseInstanceAddon(addon ovhAddon, pricing map[string]*OVHFlavorPricing) {
	planCode := addon.PlanCode
	isMonthly := strings.Contains(planCode, ".monthly.")

	// Extract flavor name: strip .consumption or .monthly.postpaid suffix
	flavorName := planCode
	if idx := strings.Index(planCode, ".consumption"); idx > 0 {
		flavorName = planCode[:idx]
	} else if idx := strings.Index(planCode, ".monthly."); idx > 0 {
		flavorName = planCode[:idx]
	}

	if len(addon.Pricings) == 0 {
		return
	}

	// Select pricing entry by type: "consumption" for hourly, "monthly.postpaid" for monthly
	targetType := "consumption"
	if isMonthly {
		targetType = "monthly.postpaid"
	}
	var rawPrice float64
	matched := false
	for _, p := range addon.Pricings {
		if p.Type == targetType {
			rawPrice = float64(p.Price) / microcentsPerUnit
			matched = true
			break
		}
	}
	if !matched {
		rawPrice = float64(addon.Pricings[0].Price) / microcentsPerUnit
	}

	entry, exists := pricing[flavorName]
	if !exists {
		entry = &OVHFlavorPricing{PlanCode: planCode}
		pricing[flavorName] = entry
	}

	if isMonthly {
		entry.MonthlyPrice = rawPrice / hoursPerMonth
	} else {
		entry.HourlyPrice = rawPrice
		// Extract specs from blobs
		if addon.Blobs != nil && addon.Blobs.Technical != nil {
			tech := addon.Blobs.Technical
			if tech.CPU != nil {
				entry.VCPU = int(tech.CPU.Cores)
			}
			if tech.Memory != nil {
				entry.RAM = int(tech.Memory.Size)
			}
			if tech.Storage != nil && len(tech.Storage.Disks) > 0 {
				entry.Disk = int(tech.Storage.Disks[0].Capacity)
			}
			if tech.GPU != nil {
				entry.GPU = tech.GPU.Number
				entry.GPUName = tech.GPU.Model
			}
		}
	}
}

// parseVolumeAddon extracts volume pricing from a volume addon entry.
func parseVolumeAddon(addon ovhAddon, volumePricing map[string]float64) {
	planCode := addon.PlanCode
	// Extract volume type: volume.high-speed-gen2.consumption -> high-speed-gen2
	parts := strings.SplitN(planCode, ".", 3)
	if len(parts) < 3 {
		return
	}
	volumeType := parts[1]

	if len(addon.Pricings) == 0 {
		return
	}

	// Only use consumption (hourly) pricing
	for _, p := range addon.Pricings {
		if p.Type == "consumption" {
			volumePricing[volumeType] = float64(p.Price) / microcentsPerUnit
			return
		}
	}
	volumePricing[volumeType] = float64(addon.Pricings[0].Price) / microcentsPerUnit
}

// ovhKey implements models.Key for OVH nodes.
type ovhKey struct {
	Labels map[string]string
}

func (k *ovhKey) Features() string {
	region, _ := util.GetRegion(k.Labels)
	instanceType, _ := util.GetInstanceType(k.Labels)
	return region + "," + instanceType
}

func (k *ovhKey) GPUType() string {
	instanceType, _ := util.GetInstanceType(k.Labels)
	for _, prefix := range gpuPrefixes {
		if strings.HasPrefix(instanceType, prefix) {
			return instanceType
		}
	}
	return ""
}

// GPUCount returns 0 as GPU count is derived from the flavor lookup in NodePricing,
// not from node labels. This is consistent with other providers.
func (k *ovhKey) GPUCount() int {
	return 0
}

func (k *ovhKey) ID() string {
	return ""
}

// ovhPVKey implements models.PVKey for OVH persistent volumes.
type ovhPVKey struct {
	StorageClassName       string
	StorageClassParameters map[string]string
	Zone                   string
}

func (k *ovhPVKey) Features() string {
	// First try the StorageClass name mapping
	volumeType := storageClassToVolumeType[k.StorageClassName]
	// Fallback to the "type" parameter from StorageClass (e.g. "high-speed-gen2")
	if volumeType == "" && k.StorageClassParameters != nil {
		volumeType = k.StorageClassParameters["type"]
	}
	return k.Zone + "," + volumeType
}

func (k *ovhPVKey) GetStorageClass() string {
	return k.StorageClassName
}

func (k *ovhPVKey) ID() string {
	return ""
}

// isMonthlyBilling determines whether a node uses monthly billing.
func isMonthlyBilling(labels map[string]string, monthlyPools []string) bool {
	if v, ok := labels[BillingLabel]; ok {
		if v == "monthly" {
			return true
		}
		if v == "hourly" {
			return false
		}
	}

	if pool, ok := labels[NodepoolLabel]; ok {
		for _, mp := range monthlyPools {
			if pool == mp {
				return true
			}
		}
	}

	return false
}

func (c *OVH) getCatalogURL() string {
	if c.catalogURL != "" {
		return c.catalogURL
	}
	u, _ := url.Parse("https://eu.api.ovh.com/v1/order/catalog/public/cloud")
	q := u.Query()
	q.Set("ovhSubsidiary", env.GetOVHSubsidiary())
	u.RawQuery = q.Encode()
	return u.String()
}

// DownloadPricingData fetches the OVH public cloud catalog and parses pricing.
func (c *OVH) DownloadPricingData() error {
	c.DownloadLock.Lock()
	defer c.DownloadLock.Unlock()

	c.monthlyNodepools = env.GetOVHMonthlyNodepools()

	if c.Pricing != nil {
		return nil
	}

	catalogURL := c.getCatalogURL()
	log.Infof("Downloading OVH pricing data from %s", catalogURL)

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Get(catalogURL)
	if err != nil {
		return fmt.Errorf("failed to fetch OVH catalog: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("OVH catalog returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read OVH catalog response: %w", err)
	}

	pricing, volumePricing, err := parseCatalog(body)
	if err != nil {
		return err
	}

	c.Pricing = pricing
	c.VolumePricing = volumePricing

	log.Infof("Loaded OVH pricing: %d flavors, %d volume types", len(pricing), len(volumePricing))
	return nil
}

// NodePricing returns pricing for a specific node based on its key.
func (c *OVH) NodePricing(key models.Key) (*models.Node, models.PricingMetadata, error) {
	c.DownloadLock.RLock()
	defer c.DownloadLock.RUnlock()

	meta := models.PricingMetadata{Source: "ovh"}

	features := strings.Split(key.Features(), ",")
	if len(features) < 2 {
		return nil, meta, fmt.Errorf("invalid key features: %s", key.Features())
	}

	region := features[0]
	instanceType := features[1]

	flavor, ok := c.Pricing[instanceType]
	if !ok {
		return nil, meta, fmt.Errorf("flavor not found in OVH pricing: %s", instanceType)
	}

	// Determine billing mode
	var labels map[string]string
	if k, ok := key.(*ovhKey); ok {
		labels = k.Labels
	}

	price := flavor.HourlyPrice
	if isMonthlyBilling(labels, c.monthlyNodepools) && flavor.MonthlyPrice > 0 {
		price = flavor.MonthlyPrice
	}

	return &models.Node{
		Cost:         fmt.Sprintf("%f", price),
		VCPU:         fmt.Sprintf("%d", flavor.VCPU),
		RAM:          fmt.Sprintf("%d", flavor.RAM),
		Storage:      fmt.Sprintf("%d", flavor.Disk),
		GPU:          fmt.Sprintf("%d", flavor.GPU),
		GPUName:      flavor.GPUName,
		InstanceType: instanceType,
		Region:       region,
		// DefaultPrices for both hourly and monthly; monthly prices are pre-amortized to hourly (/730).
		PricingType: models.DefaultPrices,
	}, meta, nil
}

// PVPricing returns pricing for a persistent volume.
func (c *OVH) PVPricing(pvk models.PVKey) (*models.PV, error) {
	c.DownloadLock.RLock()
	defer c.DownloadLock.RUnlock()

	features := strings.Split(pvk.Features(), ",")
	volumeType := ""
	if len(features) > 1 {
		volumeType = features[1]
	}

	cost, ok := c.VolumePricing[volumeType]
	if !ok {
		log.Debugf("Volume pricing not found for storage class %s (type: %s)", pvk.GetStorageClass(), volumeType)
		return &models.PV{}, nil
	}

	return &models.PV{
		Cost:  fmt.Sprintf("%f", cost),
		Class: pvk.GetStorageClass(),
	}, nil
}

// NetworkPricing returns static network pricing for OVH.
func (c *OVH) NetworkPricing() (*models.Network, error) {
	return &models.Network{
		ZoneNetworkEgressCost:     0,
		RegionNetworkEgressCost:   0,
		InternetNetworkEgressCost: 0.01,
		NatGatewayEgressCost:      0,
		NatGatewayIngressCost:     0,
	}, nil
}

// LoadBalancerPricing returns static load balancer pricing for OVH.
func (c *OVH) LoadBalancerPricing() (*models.LoadBalancer, error) {
	return &models.LoadBalancer{
		Cost: 0.012,
	}, nil
}

// GpuPricing returns GPU-specific pricing (not used for OVH).
func (c *OVH) GpuPricing(nodeLabels map[string]string) (string, error) {
	return "", nil
}

// ClusterInfo returns metadata about the cluster.
func (c *OVH) ClusterInfo() (map[string]string, error) {
	remoteEnabled := env.IsRemoteEnabled()

	m := make(map[string]string)
	m["name"] = "OVH Cluster #1"

	conf, err := c.GetConfig()
	if err != nil {
		return nil, err
	}
	if conf.ClusterName != "" {
		m["name"] = conf.ClusterName
	}

	m["provider"] = opencost.OVHProvider
	m["region"] = c.ClusterRegion
	m["account"] = c.ClusterAccountID
	m["remoteReadEnabled"] = strconv.FormatBool(remoteEnabled)
	m["id"] = coreenv.GetClusterID()
	return m, nil
}

// GetManagementPlatform detects the management platform from node labels.
func (c *OVH) GetManagementPlatform() (string, error) {
	nodes := c.Clientset.GetAllNodes()
	if len(nodes) > 0 {
		n := nodes[0]
		if _, ok := n.Labels[NodepoolLabel]; ok {
			return "mks", nil
		}
	}
	return "", nil
}

// GetKey returns a Key for matching node pricing.
func (c *OVH) GetKey(labels map[string]string, n *clustercache.Node) models.Key {
	return &ovhKey{Labels: labels}
}

// GetPVKey returns a PVKey for matching persistent volume pricing.
func (c *OVH) GetPVKey(pv *clustercache.PersistentVolume, parameters map[string]string, defaultRegion string) models.PVKey {
	zone := ""
	if pv.Spec.CSI != nil {
		parts := strings.Split(pv.Spec.CSI.VolumeHandle, "/")
		if len(parts) > 0 {
			zone = parts[0]
		}
	}
	return &ovhPVKey{
		StorageClassName:       pv.Spec.StorageClassName,
		StorageClassParameters: parameters,
		Zone:                   zone,
	}
}

// GetAddresses is not implemented for OVH.
func (c *OVH) GetAddresses() ([]byte, error) {
	return nil, nil
}

// GetDisks is not implemented for OVH.
func (c *OVH) GetDisks() ([]byte, error) {
	return nil, nil
}

// GetOrphanedResources is not implemented for OVH.
func (c *OVH) GetOrphanedResources() ([]models.OrphanedResource, error) {
	return nil, errors.New("not implemented")
}

// AllNodePricing returns all cached node pricing data.
func (c *OVH) AllNodePricing() (interface{}, error) {
	c.DownloadLock.RLock()
	defer c.DownloadLock.RUnlock()
	return c.Pricing, nil
}

// UpdateConfigFromConfigMap updates config from a ConfigMap.
func (c *OVH) UpdateConfigFromConfigMap(a map[string]string) (*models.CustomPricing, error) {
	return c.Config.UpdateFromMap(a)
}

// UpdateConfig updates custom pricing from a JSON reader.
func (c *OVH) UpdateConfig(r io.Reader, updateType string) (*models.CustomPricing, error) {
	defer c.DownloadPricingData()

	return c.Config.Update(func(cp *models.CustomPricing) error {
		a := make(map[string]interface{})
		err := coreJSON.NewDecoder(r).Decode(&a)
		if err != nil {
			return err
		}
		for k, v := range a {
			kUpper := utils.ToTitle.String(k)
			vstr, ok := v.(string)
			if ok {
				err := models.SetCustomPricingField(cp, kUpper, vstr)
				if err != nil {
					return fmt.Errorf("error setting custom pricing field: %w", err)
				}
			} else {
				return fmt.Errorf("type error while updating config for %s", kUpper)
			}
		}

		if env.IsRemoteEnabled() {
			err := utils.UpdateClusterMeta(coreenv.GetClusterID(), cp.ClusterName)
			if err != nil {
				return err
			}
		}

		return nil
	})
}

// GetConfig returns the custom pricing configuration with OVH defaults.
func (c *OVH) GetConfig() (*models.CustomPricing, error) {
	cp, err := c.Config.GetCustomPricingData()
	if err != nil {
		return nil, err
	}
	if cp.Discount == "" {
		cp.Discount = "0%"
	}
	if cp.NegotiatedDiscount == "" {
		cp.NegotiatedDiscount = "0%"
	}
	if cp.CurrencyCode == "" {
		cp.CurrencyCode = "EUR"
	}
	return cp, nil
}

// ClusterManagementPricing returns the management cost for the cluster.
func (c *OVH) ClusterManagementPricing() (string, float64, error) {
	return "", 0.0, nil
}

// CombinedDiscountForNode calculates the combined discount for a node.
func (c *OVH) CombinedDiscountForNode(instanceType string, isPreemptible bool, defaultDiscount, negotiatedDiscount float64) float64 {
	return 1.0 - ((1.0 - defaultDiscount) * (1.0 - negotiatedDiscount))
}

// Regions returns the list of supported OVH regions.
func (c *OVH) Regions() []string {
	regionOverrides := env.GetRegionOverrideList()
	if len(regionOverrides) > 0 {
		log.Debugf("Overriding OVH regions with configured region list: %+v", regionOverrides)
		return regionOverrides
	}
	return ovhRegions
}

// ApplyReservedInstancePricing is a no-op for OVH.
func (c *OVH) ApplyReservedInstancePricing(nodes map[string]*models.Node) {}

// ServiceAccountStatus returns the service account status.
func (c *OVH) ServiceAccountStatus() *models.ServiceAccountStatus {
	return &models.ServiceAccountStatus{
		Checks: []*models.ServiceAccountCheck{},
	}
}

// PricingSourceStatus returns the status of the pricing data source.
func (c *OVH) PricingSourceStatus() map[string]*models.PricingSource {
	return map[string]*models.PricingSource{
		OVHCatalogPricing: {
			Name:      OVHCatalogPricing,
			Enabled:   true,
			Available: true,
		},
	}
}

// PricingSourceSummary returns the parsed pricing data.
func (c *OVH) PricingSourceSummary() interface{} {
	return c.Pricing
}
