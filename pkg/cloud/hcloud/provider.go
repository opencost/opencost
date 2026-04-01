package hcloud

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"

	"github.com/hetznercloud/hcloud-go/v2/hcloud"
	"github.com/opencost/opencost/core/pkg/clustercache"
	coreenv "github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/version"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/env"
)

// HCloud implements the models.Provider interface for Hetzner Cloud.
type HCloud struct {
	Clientset        clustercache.ClusterCache
	Config           models.ProviderConfig
	ClusterRegion    string
	ClusterAccountID string
	client           *hcloud.Client
	pricing          *hcloud.Pricing
	pricingErr       error
	pricingMu        sync.Mutex
}

// defaultHCloud regions is used as a fallback, when the GET /locations API call fails.
// Retrieved using the hcloud CLI tool on 2026-04-1 by running `hcloud location list`.
var defaultHCloudRegions = []string{
	"fsn1", // Falkenstein DC Park 1 (eu-central)
	"nbg1", // Nuremberg DC Park 1 (eu-central)
	"hel1", // Helsinki DC Park 1 (eu-central)
	"ash",  // Ashburn, VA (us-east)
	"hil",  // Hillsboro, OR (us-west)
	"sin",  // Singapore (ap-southeast)
}

// NewHCloudProvider creates a new HCloud provider with the given configuration.
func NewHCloudProvider(cache clustercache.ClusterCache, region string, accountID string, config models.ProviderConfig) *HCloud {
	token := env.GetHcloudToken()

	client := hcloud.NewClient(
		hcloud.WithToken(token),
		hcloud.WithApplication("opencost", version.Version),
	)

	return &HCloud{
		Clientset:        cache,
		Config:           config,
		ClusterRegion:    region,
		ClusterAccountID: accountID,
		client:           client,
	}
}

// fetchPricing retrieves the pricing information from the Hetzner Cloud API, with caching to avoid redundant API calls.
func (h *HCloud) fetchPricing(ctx context.Context) (*hcloud.Pricing, error) {
	h.pricingMu.Lock()
	defer h.pricingMu.Unlock()

	if h.pricing != nil {
		return h.pricing, nil
	}

	pricing, _, err := h.client.Pricing.Get(ctx)
	if err != nil {
		h.pricingErr = err
		return nil, err
	}

	h.pricing = &pricing
	h.pricingErr = nil

	return h.pricing, nil
}

// AllNodePricing implements [models.Provider]. Returns all pricing data as [*hcloud.Pricing]. Fetches data from the Hetzner Cloud API if it is not already cached.
func (h *HCloud) AllNodePricing() (interface{}, error) {
	pricing, err := h.fetchPricing(context.Background())
	if err != nil {
		return nil, err
	}

	return pricing, nil
}

// ApplyReservedInstancePricing implements [models.Provider]. Hetzner Cloud does not have reserved instances, so this is a no-op.
func (h *HCloud) ApplyReservedInstancePricing(map[string]*models.Node) {
}

// ClusterInfo implements [models.Provider]. Returns metadata about the cluster.
func (h *HCloud) ClusterInfo() (map[string]string, error) {
	remoteEnabled := env.IsRemoteEnabled()

	m := make(map[string]string)
	m["name"] = "Hetzner Cloud Cluster"

	conf, err := h.GetConfig()
	if err != nil {
		return nil, err
	}
	if conf.ClusterName != "" {
		m["name"] = conf.ClusterName
	}

	m["provider"] = opencost.HCloudProvider
	m["region"] = h.ClusterRegion
	m["account"] = h.ClusterAccountID
	m["remoteReadEnabled"] = strconv.FormatBool(remoteEnabled)
	m["id"] = coreenv.GetClusterID()

	return m, nil
}

// ClusterManagementPricing implements [models.Provider]. Returns the management cost for the cluster.
func (h *HCloud) ClusterManagementPricing() (string, float64, error) {
	// There is no managed kubernetes offering from Hetzner Cloud yet, so we return 0 for the management cost.
	return "", 0, nil
}

// CombinedDiscountForNode implements [models.Provider]. Calculates the combined discount for a node.
func (h *HCloud) CombinedDiscountForNode(string, bool, float64, float64) float64 {
	// Hetzner Cloud does not have reserved instances or negotiated discounts, so we return 0 for the combined discount.
	return 0
}

// DownloadPricingData implements [models.Provider]. Fetches the latest pricing data from the Hetzner Cloud API and stores it in the cache.
func (h *HCloud) DownloadPricingData() error {
	_, err := h.fetchPricing(context.Background())
	return err
}

// GetAddresses implements [models.Provider]. Not implemented for Hetzner Cloud.
func (h *HCloud) GetAddresses() ([]byte, error) {
	return nil, errors.New("not implemented")
}

// GetConfig implements [models.Provider]. Returns the custom pricing configuration.
func (h *HCloud) GetConfig() (*models.CustomPricing, error) {
	pricing, err := h.fetchPricing(context.Background())
	if err != nil {
		return nil, err
	}

	c, err := h.Config.GetCustomPricingData()
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
		c.CurrencyCode = pricing.Currency
	}

	return c, nil
}

// GetDisks implements [models.Provider]. Not Implemented for Hetzner Cloud.
func (h *HCloud) GetDisks() ([]byte, error) {
	return nil, nil
}

// GetKey implements [models.Provider]. Returns a Key for matching node pricing.
func (h *HCloud) GetKey(labels map[string]string, n *clustercache.Node) models.Key {
	return &hcloudKey{
		providerID: n.SpecProviderID,
		labels:     labels,
	}
}

// GetManagementPlatform implements [models.Provider].
func (h *HCloud) GetManagementPlatform() (string, error) {
	// Hetzner Cloud does not have a managed Kubernetes offering, so we return an empty string for the management platform.
	return "", nil
}

// GetOrphanedResources implements [models.Provider].
func (h *HCloud) GetOrphanedResources() ([]models.OrphanedResource, error) {
	return nil, errors.New("not implemented")
}

// GetPVKey implements [models.Provider]. Returns a PVKey for matching persistent volume pricing.
func (h *HCloud) GetPVKey(pv *clustercache.PersistentVolume, parameters map[string]string, defaultRegion string) models.PVKey {
	// Region is in node affinity
	region := ""
	if pv.Spec.NodeAffinity != nil && pv.Spec.NodeAffinity.Required != nil {
		for _, term := range pv.Spec.NodeAffinity.Required.NodeSelectorTerms {
			for _, expr := range term.MatchExpressions {
				if expr.Key == "csi.hetzner.cloud/location" && len(expr.Values) > 0 {
					region = expr.Values[0]
					break
				}
			}
		}
	}

	return &hcloudPVKey{
		providerID:   pv.Spec.CSI.VolumeHandle,
		region:       region,
		sizeBytes:    pv.Spec.Capacity.Storage().Value(),
		storageClass: pv.Spec.StorageClassName,
	}
}

// GpuPricing implements [models.Provider]. Returns an empty string, as Hetzner Cloud does not currently offer GPU instances.
func (h *HCloud) GpuPricing(map[string]string) (string, error) {
	return "", nil
}

// LoadBalancerPricing implements [models.Provider]. Returns the hourly cost of a Hetzner Cloud Load Balancer.
// Opencost does not currently pass individual Load Balancer characteristics (like annotations).
// Wea assume that an lb11 load balancer in fsn1 is used.
func (h *HCloud) LoadBalancerPricing() (*models.LoadBalancer, error) {
	var lbType = "lb11"
	var lbLocation = "fsn1"

	pricing, err := h.fetchPricing(context.Background())
	if err != nil {
		return nil, err
	}

	lb := models.LoadBalancer{}
	for _, lbPricing := range pricing.LoadBalancerTypes {
		if lbPricing.LoadBalancerType.Name == lbType {
			for _, locationPricing := range lbPricing.Pricings {
				if locationPricing.Location.Name == lbLocation {
					f, err := strconv.ParseFloat(locationPricing.Hourly.Gross, 64)
					if err != nil {
						return nil, err
					}
					lb.Cost = f
					break
				}
			}
			break
		}
	}

	return &lb, nil
}

// NetworkPricing implements [models.Provider]. Returns static network pricing for Hetzner Cloud.
func (h *HCloud) NetworkPricing() (*models.Network, error) {
	return &models.Network{
		ZoneNetworkEgressCost:     0,
		RegionNetworkEgressCost:   0,
		InternetNetworkEgressCost: 0,
		NatGatewayEgressCost:      0,
		NatGatewayIngressCost:     0,
	}, nil
}

// NodePricing implements [models.Provider]. Returns hourly pricing for a specific node based on its key.
func (h *HCloud) NodePricing(key models.Key) (*models.Node, models.PricingMetadata, error) {
	meta := models.PricingMetadata{}

	pricing, err := h.fetchPricing(context.Background())
	if err != nil {
		return nil, meta, err
	}

	meta.Currency = pricing.Currency
	meta.Source = "Hetzner Cloud API"

	var region, instanceType string
	var serverType *hcloud.ServerType
	var locationPricing *hcloud.ServerTypeLocationPricing

	// Fetch pricing information via GET /v1/servers/{id} API call.
	// If that fails, continue to lookup pricing based on the key features.
	// As of now (2026-04-01), the Hetzner pricing API (GET /v1/pricing) does not return information about all server types.
	serverID := key.ID()
	if serverID == "" {
		log.Infof("node with empty ID, cannot fetch pricing from API, falling back to default pricing lookup using key features: %s", key.Features())
	} else {
		serverIDNum, err := strconv.ParseInt(strings.TrimPrefix(serverID, "hcloud://"), 10, 64)
		if err != nil {
			log.Infof("failed to parse node ID %s, falling back to default pricing lookup using key features: %s", serverID, key.Features())
		} else {
			server, _, err := h.client.Server.GetByID(context.Background(), serverIDNum)
			if err != nil {
				log.Infof("failed to fetch server details for node ID %s, falling back to default pricing lookup using key features: %s", serverID, key.Features())
			} else {
				region = server.Location.Name
				instanceType = server.ServerType.Name
				for _, lp := range server.ServerType.Pricings {
					if lp.Location.Name == region {
						serverType = server.ServerType
						locationPricing = &lp
						break
					}
				}
			}
		}
	}

	// Lookup pricing based on key features.
	if locationPricing == nil {
		features := strings.Split(key.Features(), ",")
		if len(features) != 2 {
			return nil, meta, fmt.Errorf("invalid key features: %s", key.Features())
		}
		region = features[0]
		instanceType = features[1]

		for _, stp := range pricing.ServerTypes {
			if stp.ServerType.Name == instanceType {
				for _, lp := range stp.Pricings {
					if lp.Location.Name == region {
						serverType = stp.ServerType
						locationPricing = &lp
						break
					}
				}
				break
			}
		}
	}

	if locationPricing == nil {
		return nil, meta, fmt.Errorf("pricing not found for instance type %s in region %s", instanceType, region)
	}

	node := models.Node{
		Cost:         locationPricing.Hourly.Gross,
		VCPU:         fmt.Sprintf("%d", serverType.Cores),
		RAM:          fmt.Sprintf("%.0f", serverType.Memory), // in GB
		Storage:      fmt.Sprintf("%d", serverType.Disk),     // in GB
		InstanceType: instanceType,
		Region:       region,
		ProviderID:   key.ID(),
		PricingType:  models.DefaultPrices,
		ArchType:     string(serverType.Architecture),
	}

	return &node, meta, nil
}

// PVPricing implements [models.Provider]. Returns hourly pricing for a specific persistent volume based on its key.
func (h *HCloud) PVPricing(pvKey models.PVKey) (*models.PV, error) {
	pricing, err := h.fetchPricing(context.Background())
	if err != nil {
		return nil, err
	}

	features := strings.Split(pvKey.Features(), ",")
	if len(features) != 2 {
		return nil, fmt.Errorf("invalid key features: %s", pvKey.Features())
	}
	region := features[0]
	sizeInBytes, err := strconv.ParseFloat(features[1], 64)
	if err != nil {
		return nil, fmt.Errorf("invalid size in bytes: %s", features[0])
	}

	pricePerGBMonthly, err := strconv.ParseFloat(pricing.Volume.PerGBMonthly.Gross, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid price per GB monthly: %s", pricing.Volume.PerGBMonthly.Gross)
	}

	monthlyCost := (sizeInBytes / 1_000_000_000) * pricePerGBMonthly
	hourlyCost := monthlyCost / (30 * 24) // Assuming 30 days in a month

	return &models.PV{
		Cost:       fmt.Sprintf("%f", hourlyCost),
		Class:      pvKey.GetStorageClass(),
		Region:     region,
		ProviderID: pvKey.ID(),
	}, nil
}

// PricingSourceStatus implements [models.Provider]. Returns the status of the pricing data source.
func (h *HCloud) PricingSourceStatus() map[string]*models.PricingSource {
	var available = false
	var sourceError string

	h.pricingMu.Lock()
	if h.pricing != nil {
		available = true
	}
	if h.pricingErr != nil {
		sourceError = h.pricingErr.Error()
	}
	h.pricingMu.Unlock()

	return map[string]*models.PricingSource{
		"HCloudPricing": {
			Name:      "Hetzner Cloud Pricing",
			Enabled:   true,
			Available: available,
			Error:     sourceError,
		},
	}
}

// PricingSourceSummary implements [models.Provider]. Returns all pricing data as [*hcloud.Pricing], if it was previously fetched successfully. Otherwise, returns nil.
func (h *HCloud) PricingSourceSummary() interface{} {
	return h.pricing
}

// Regions implements [models.Provider]. Returns a list of Hetzner Cloud locations. If the API call fails, returns a default list of regions.
func (h *HCloud) Regions() []string {
	regionOverrides := env.GetRegionOverrideList()

	if len(regionOverrides) > 0 {
		log.Debugf("Overriding GCP regions with configured region list: %+v", regionOverrides)
		return regionOverrides
	}

	locations, err := h.client.Location.All(context.Background())
	if err != nil {
		log.Warnf("failed to fetch regions from Hetzner Cloud API, falling back to default region list: %w", err)
		return defaultHCloudRegions
	}

	regions := make([]string, len(locations))
	for i, location := range locations {
		regions[i] = location.Name
	}

	return regions
}

// ServiceAccountStatus implements [models.Provider]. This is a no-op for Hetzner Cloud and just returns an empty status.
func (h *HCloud) ServiceAccountStatus() *models.ServiceAccountStatus {
	return &models.ServiceAccountStatus{
		Checks: []*models.ServiceAccountCheck{},
	}
}

// UpdateConfig implements [models.Provider].
func (h *HCloud) UpdateConfig(r io.Reader, updateType string) (*models.CustomPricing, error) {
	return nil, errors.New("not implemented")
}

// UpdateConfigFromConfigMap implements [models.Provider].
func (h *HCloud) UpdateConfigFromConfigMap(a map[string]string) (*models.CustomPricing, error) {
	return h.Config.UpdateFromMap(a)
}

// Interface guard
var _ models.Provider = (*HCloud)(nil)
