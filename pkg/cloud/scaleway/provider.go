package scaleway

import (
	"errors"
	"fmt"
	"io"
	"maps"
	"strconv"
	"strings"
	"sync"

	coreenv "github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/cloud/utils"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/pkg/env"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/scaleway/scaleway-sdk-go/api/instance/v1"
	"github.com/scaleway/scaleway-sdk-go/scw"
)

const (
	InstanceAPIPricing = "Instance API Pricing"
)

type ScalewayPricing struct {
	NodesInfos map[string]*instance.ServerType
	PVCost     float64
}

type Scaleway struct {
	Clientset               clustercache.ClusterCache
	Config                  models.ProviderConfig
	Pricing                 map[string]*ScalewayPricing
	ClusterRegion           string
	ClusterAccountID        string
	DownloadPricingDataLock sync.RWMutex

	// Catalog is the catalog-backed pricing store (see catalog.go). It is
	// swapped in atomically under DownloadPricingDataLock and is nil until
	// the first successful fetch (edge case 1: a failed fetch never wipes it).
	Catalog        *catalogStore
	catalogFetched bool // true iff the last full catalog fetch completed
	catalogError   string
}

// PricingSourceSummary returns the pricing source summary for the provider.
// The summary represents what was _parsed_ from the pricing source, not
// everything that was _available_ in the pricing source.
func (c *Scaleway) PricingSourceSummary() any {
	c.DownloadPricingDataLock.RLock()
	defer c.DownloadPricingDataLock.RUnlock()

	// The existing per-zone parsed pricing is returned unchanged; the catalog
	// section is additive (contracts/pricing-source-status.md §2).
	summary := make(map[string]any, len(c.Pricing)+1)
	for zone, pricing := range c.Pricing {
		summary[zone] = pricing
	}
	if c.Catalog != nil {
		summary["catalog"] = c.Catalog.summary()
	}
	return summary
}

func (c *Scaleway) DownloadPricingData() error {
	c.DownloadPricingDataLock.Lock()
	defer c.DownloadPricingDataLock.Unlock()

	// The Product Catalog is the primary pricing source. It is re-fetched on
	// every download — at startup and via the manual refresh trigger (FR-001,
	// SC-002). A failure keeps the previous store and never blocks the legacy
	// sources below (edge case 1, FR-007).
	cfg, _ := c.GetConfig()
	currency := "EUR"
	if cfg != nil && cfg.CurrencyCode != "" {
		currency = cfg.CurrencyCode
	}
	catalog, err := fetchCatalog(currency)
	if err != nil {
		c.catalogFetched = false
		c.catalogError = err.Error()
		log.Errorf("Could not fetch Scaleway Product Catalog, keeping previous pricing: %s", err)
	} else {
		c.Catalog = catalog
		c.catalogFetched = true
		c.catalogError = ""
		registerCatalogCarbonCoefficients(catalog)
	}

	if len(c.Pricing) != 0 {
		// Legacy pricing already initialized on a previous download.
		return nil
	}

	// TODO wait for an official Pricing API from Scaleway
	// Let's use a static map and an old API

	// PV pricing per AZ
	pvPrice := map[string]float64{
		"fr-par-1": 0.00011,
		"fr-par-2": 0.00011,
		"fr-par-3": 0.00032,
		"nl-ams-1": 0.00008,
		"nl-ams-2": 0.00008,
		"nl-ams-3": 0.00008,
		"pl-waw-1": 0.00011,
		"pl-waw-2": 0.00011,
		"pl-waw-3": 0.00011,
	}

	c.Pricing = make(map[string]*ScalewayPricing)

	// The endpoint we are trying to hit does not have authentication
	client, err := scw.NewClient(scw.WithoutAuth())
	if err != nil {
		if c.Catalog == nil {
			return fmt.Errorf("no Scaleway pricing data available: catalog fetch failed: %s; instance client creation failed: %w", c.catalogError, err)
		}
		return err
	}

	instanceAPI := instance.NewAPI(client)

	for _, zone := range scw.AllZones {
		resp, err := instanceAPI.ListServersTypes(&instance.ListServersTypesRequest{Zone: zone})
		if err != nil {
			log.Errorf("Could not get Scaleway pricing data from instance API in zone %s: %+v", zone, err)
			continue
		}
		c.Pricing[zone.String()] = &ScalewayPricing{
			PVCost:     pvPrice[zone.String()],
			NodesInfos: map[string]*instance.ServerType{},
		}

		maps.Copy(c.Pricing[zone.String()].NodesInfos, resp.Servers)
	}

	if len(c.Pricing) == 0 && c.Catalog == nil {
		return fmt.Errorf("no Scaleway pricing data available: catalog fetch failed: %s; instance API returned no zones", c.catalogError)
	}

	return nil
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
	split := strings.Split(key.Features(), ",")
	zone, instanceType := split[0], split[1]

	var node *models.Node

	// FR-002: the catalog is the primary node pricing source. Hardware
	// details (VCPU/RAM/Storage/GPU) are enriched from the instance API when
	// that entry exists for the same (zone, type).
	if c.Catalog != nil {
		if catalogPrice, ok := c.Catalog.instancePrice(zone, instanceType); ok {
			node = &models.Node{
				Cost:         formatPrice(catalogPrice),
				PricingType:  models.DefaultPrices,
				InstanceType: instanceType,
				Region:       zone,
				GPUName:      key.GPUType(),
			}
			if pricing, ok := c.Pricing[zone]; ok {
				if info, ok := pricing.NodesInfos[instanceType]; ok {
					node.VCPU = fmt.Sprintf("%d", info.Ncpus)
					node.RAM = fmt.Sprintf("%d", info.RAM)
					// This is tricky, as instances can have local volumes or not
					node.Storage = fmt.Sprintf("%d", info.PerVolumeConstraint.LSSD.MinSize)
					node.GPU = fmt.Sprintf("%d", *info.Gpu)
				}
			}
		}
	}

	if node == nil {
		// FR-007: fall back to the existing per-instance pricing source (edge case 2).
		if pricing, ok := c.Pricing[zone]; ok {
			if info, ok := pricing.NodesInfos[instanceType]; ok {
				log.DedupedWarningf(10, "Scaleway: no catalog pricing for %s in zone %s, falling back to instance API pricing", instanceType, zone)
				node = &models.Node{
					Cost:        fmt.Sprintf("%f", info.HourlyPrice),
					PricingType: models.DefaultPrices,
					VCPU:        fmt.Sprintf("%d", info.Ncpus),
					RAM:         fmt.Sprintf("%d", info.RAM),
					// This is tricky, as instances can have local volumes or not
					Storage:      fmt.Sprintf("%d", info.PerVolumeConstraint.LSSD.MinSize),
					GPU:          fmt.Sprintf("%d", *info.Gpu),
					InstanceType: instanceType,
					Region:       zone,
					GPUName:      key.GPUType(),
				}
			}
		}
	}

	if node == nil {
		return nil, meta, fmt.Errorf("Unable to find node pricing matching thes features `%s`", key.Features())
	}
	return node, meta, nil
}

func (c *Scaleway) LoadBalancerPricing() (*models.LoadBalancer, error) {
	c.DownloadPricingDataLock.RLock()
	defer c.DownloadPricingDataLock.RUnlock()

	// Different LB types exist in the catalog, but we can't get the type without
	// a service specifying the type in the annotations (R7), so we use the
	// smallest (cheapest) catalog LB node price for the cluster's zone.
	if c.Catalog != nil {
		zone := c.clusterZone()
		if price, ok := c.Catalog.loadBalancerPrice(zone); ok {
			return &models.LoadBalancer{
				Cost: price,
			}, nil
		}
		log.DedupedWarningf(10, "Scaleway: no catalog load balancer pricing for zone %s, falling back to static pricing", zone)
	}
	return &models.LoadBalancer{
		Cost: 0.014,
	}, nil
}

// clusterZone resolves the cluster's zone from cached node labels, when
// available (used for catalog lookups that require a zone).
func (c *Scaleway) clusterZone() string {
	if c.Clientset == nil {
		return ""
	}
	for _, n := range c.Clientset.GetAllNodes() {
		if zone, ok := util.GetZone(n.Labels); ok {
			return zone
		}
	}
	return ""
}

func (c *Scaleway) NetworkPricing() (*models.Network, error) {
	// it's free baby!
	return &models.Network{
		ZoneNetworkEgressCost:     0,
		RegionNetworkEgressCost:   0,
		InternetNetworkEgressCost: 0,
		NatGatewayEgressCost:      0,
		NatGatewayIngressCost:     0,
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

	zone := pvk.Features()
	class := pvk.GetStorageClass()

	// FR-003: the catalog is the primary volume pricing source. A miss
	// (unknown class or zone) falls back to the static per-zone price,
	// preserving today's behavior exactly (edge case 3, FR-007).
	if c.Catalog != nil {
		if price, ok := c.Catalog.volumePrice(zone, class); ok {
			return &models.PV{
				Cost:  formatPrice(price),
				Class: class,
			}, nil
		}
	}

	pricing, ok := c.Pricing[zone]
	if !ok {
		log.Debugf("Persistent Volume pricing not found for %s: %s", class, zone)
		return &models.PV{}, nil
	}
	if c.Catalog != nil {
		log.DedupedWarningf(10, "Scaleway: no catalog volume pricing for class %s in zone %s, falling back to static pricing", class, zone)
	}
	return &models.PV{
		Cost:  fmt.Sprintf("%f", pricing.PVCost),
		Class: class,
	}, nil
}

func (c *Scaleway) ServiceAccountStatus() *models.ServiceAccountStatus {
	return &models.ServiceAccountStatus{
		Checks: []*models.ServiceAccountCheck{},
	}
}

func (c *Scaleway) ClusterManagementPricing() (string, float64, error) {
	c.DownloadPricingDataLock.RLock()
	defer c.DownloadPricingDataLock.RUnlock()

	platform, _ := c.GetManagementPlatform()
	if platform != "kapsule" {
		// Not a managed control plane we price: keep the previous zero-cost
		// behavior (contract: ClusterManagementPricing §4).
		return "", 0.0, nil
	}

	region := c.ClusterRegion
	if region == "" {
		region = regionFromZone(c.clusterZone())
	}

	// FR-004: the catalog Kapsule mutualized control plane price is the value;
	// 0 is a valid catalog-sourced price for the mutualized tier (R8).
	if c.Catalog != nil {
		if price, ok := c.Catalog.controlPlanePrice(region); ok {
			return "kapsule", price, nil
		}
	}
	log.DedupedWarningf(10, "Scaleway: no catalog control plane pricing for region %s, reporting zero", region)
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
	if scw.Clientset == nil {
		return "", nil
	}
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

	return map[string]*models.PricingSource{
		InstanceAPIPricing: {
			Name:      InstanceAPIPricing,
			Enabled:   true,
			Available: true,
		},
		// FR-011: report the catalog as a distinct, always-enabled source.
		// Available is true iff the last full fetch completed (contract
		// pricing-source-status.md §1 state table); the last fetch error is
		// surfaced when unavailable (edge case 1: last-good data is still
		// served from the store, so degradation is observable without logs).
		ProductCatalogPricing: {
			Name:      ProductCatalogPricing,
			Enabled:   true,
			Available: c.catalogFetched,
			Error:     c.catalogError,
		},
	}
}
