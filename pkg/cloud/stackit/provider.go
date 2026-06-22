package stackit

import (
	"errors"
	"fmt"
	"io"
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
)

const (
	StackitPIMPricingSource = "STACKIT PIM API Pricing"
)

type STACKIT struct {
	Clientset               clustercache.ClusterCache
	Config                  models.ProviderConfig
	ClusterRegion           string
	ClusterAccountID        string
	DownloadPricingDataLock sync.RWMutex

	// PIM API pricing cache (protected by DownloadPricingDataLock)
	pimFlavors map[string]*pimFlavorPricing
	pimStorage map[string]*pimStoragePricing
}

func (s *STACKIT) PricingSourceSummary() any {
	s.DownloadPricingDataLock.RLock()
	defer s.DownloadPricingDataLock.RUnlock()
	return s.pimFlavors
}

func (s *STACKIT) DownloadPricingData() error {
	s.DownloadPricingDataLock.Lock()
	defer s.DownloadPricingDataLock.Unlock()

	flavors, storage, err := downloadPIMPricing()
	if err != nil {
		return fmt.Errorf("STACKIT: failed to download pricing from PIM API: %w", err)
	}

	if len(flavors) == 0 {
		return fmt.Errorf("STACKIT: PIM API returned no VM flavor pricing data")
	}

	s.pimFlavors = flavors
	s.pimStorage = storage
	return nil
}

func (s *STACKIT) AllNodePricing() (any, error) {
	s.DownloadPricingDataLock.RLock()
	defer s.DownloadPricingDataLock.RUnlock()
	return s.pimFlavors, nil
}

type stackitKey struct {
	Labels map[string]string
}

func (k *stackitKey) Features() string {
	instanceType, _ := util.GetInstanceType(k.Labels)
	zone, _ := util.GetZone(k.Labels)
	return zone + "," + instanceType
}

func (k *stackitKey) GPUCount() int {
	return 0
}

func (k *stackitKey) GPUType() string {
	return ""
}

func (k *stackitKey) ID() string {
	return ""
}

func (s *STACKIT) NodePricing(key models.Key) (*models.Node, models.PricingMetadata, error) {
	s.DownloadPricingDataLock.RLock()
	defer s.DownloadPricingDataLock.RUnlock()

	meta := models.PricingMetadata{}

	// Extract instance type from key features ("zone,instanceType")
	features := key.Features()
	parts := strings.Split(features, ",")
	instanceType := ""
	if len(parts) >= 2 {
		instanceType = parts[1]
	}

	pf, ok := s.pimFlavors[instanceType]
	if !ok {
		return nil, meta, fmt.Errorf("STACKIT: no pricing data found for instance type %q", instanceType)
	}

	ramBytes := int64(pf.RAMGB * 1024 * 1024 * 1024)
	return &models.Node{
		Cost:         pf.HourlyCost,
		VCPU:         fmt.Sprintf("%d", pf.VCPU),
		RAM:          fmt.Sprintf("%g", pf.RAMGB),
		RAMBytes:     fmt.Sprintf("%d", ramBytes),
		GPU:          fmt.Sprintf("%d", pf.GPUCount),
		GPUName:      pf.GPUType,
		InstanceType: instanceType,
		Region:       s.ClusterRegion,
		PricingType:  models.DefaultPrices,
	}, meta, nil
}

func (s *STACKIT) LoadBalancerPricing() (*models.LoadBalancer, error) {
	config, err := s.GetConfig()
	if err != nil {
		return nil, fmt.Errorf("unable to get config: %w", err)
	}

	lbPrice := 0.0
	if config.DefaultLBPrice != "" {
		lbPrice, _ = strconv.ParseFloat(config.DefaultLBPrice, 64)
	}

	return &models.LoadBalancer{
		Cost: lbPrice,
	}, nil
}

func (s *STACKIT) NetworkPricing() (*models.Network, error) {
	config, err := s.GetConfig()
	if err != nil {
		return nil, fmt.Errorf("unable to get config: %w", err)
	}

	zoneEgress, _ := strconv.ParseFloat(config.ZoneNetworkEgress, 64)
	regionEgress, _ := strconv.ParseFloat(config.RegionNetworkEgress, 64)
	internetEgress, _ := strconv.ParseFloat(config.InternetNetworkEgress, 64)
	natEgress, _ := strconv.ParseFloat(config.NatGatewayEgress, 64)
	natIngress, _ := strconv.ParseFloat(config.NatGatewayIngress, 64)

	return &models.Network{
		ZoneNetworkEgressCost:     zoneEgress,
		RegionNetworkEgressCost:   regionEgress,
		InternetNetworkEgressCost: internetEgress,
		NatGatewayEgressCost:      natEgress,
		NatGatewayIngressCost:     natIngress,
	}, nil
}

func (s *STACKIT) GetKey(l map[string]string, n *clustercache.Node) models.Key {
	return &stackitKey{
		Labels: l,
	}
}

type stackitPVKey struct {
	Labels                 map[string]string
	StorageClassName       string
	StorageClassParameters map[string]string
	Name                   string
	Zone                   string
}

func (key *stackitPVKey) ID() string {
	return ""
}

func (key *stackitPVKey) GetStorageClass() string {
	return key.StorageClassName
}

func (key *stackitPVKey) Features() string {
	return key.Zone
}

func (s *STACKIT) GetPVKey(pv *clustercache.PersistentVolume, parameters map[string]string, defaultRegion string) models.PVKey {
	zone := defaultRegion
	if pv.Spec.CSI != nil {
		parts := strings.Split(pv.Spec.CSI.VolumeHandle, "/")
		if len(parts) >= 2 && parts[0] != "" {
			zone = parts[0]
		}
	}
	return &stackitPVKey{
		Labels:                 pv.Labels,
		StorageClassName:       pv.Spec.StorageClassName,
		StorageClassParameters: parameters,
		Name:                   pv.Name,
		Zone:                   zone,
	}
}

func (s *STACKIT) GpuPricing(nodeLabels map[string]string) (string, error) {
	s.DownloadPricingDataLock.RLock()
	defer s.DownloadPricingDataLock.RUnlock()

	instanceType, _ := util.GetInstanceType(nodeLabels)
	pf, ok := s.pimFlavors[instanceType]
	if !ok || pf.GPUCount == 0 || pf.HourlyCost == "" {
		return "", nil
	}

	hourlyCost, err := strconv.ParseFloat(pf.HourlyCost, 64)
	if err != nil {
		return "", fmt.Errorf("parsing STACKIT GPU hourly cost %q for %q: %w", pf.HourlyCost, instanceType, err)
	}

	perGPUCost := hourlyCost / float64(pf.GPUCount)
	return strconv.FormatFloat(perGPUCost, 'f', -1, 64), nil
}

func (s *STACKIT) PVPricing(pvk models.PVKey) (*models.PV, error) {
	s.DownloadPricingDataLock.RLock()
	defer s.DownloadPricingDataLock.RUnlock()

	storageClass := pvk.GetStorageClass()

	if len(s.pimStorage) > 0 {
		// Exact storage class match
		if sp, ok := s.pimStorage[storageClass]; ok {
			return &models.PV{
				Cost:  sp.CostPerGBHr,
				Class: storageClass,
			}, nil
		}
		// Default to cheapest capacity-based storage
		if sp, ok := s.pimStorage["default"]; ok {
			return &models.PV{
				Cost:  sp.CostPerGBHr,
				Class: storageClass,
			}, nil
		}
	}

	log.Debugf("STACKIT: no PV pricing found for storage class %q", storageClass)
	return &models.PV{}, nil
}

func (s *STACKIT) ServiceAccountStatus() *models.ServiceAccountStatus {
	return &models.ServiceAccountStatus{
		Checks: []*models.ServiceAccountCheck{},
	}
}

func (*STACKIT) ClusterManagementPricing() (string, float64, error) {
	return "", 0.0, nil
}

func (s *STACKIT) CombinedDiscountForNode(instanceType string, isPreemptible bool, defaultDiscount, negotiatedDiscount float64) float64 {
	return 1.0 - ((1.0 - defaultDiscount) * (1.0 - negotiatedDiscount))
}

func (s *STACKIT) Regions() []string {
	regionOverrides := env.GetRegionOverrideList()
	if len(regionOverrides) > 0 {
		log.Debugf("Overriding STACKIT regions with configured region list: %+v", regionOverrides)
		return regionOverrides
	}
	return []string{"eu01"}
}

func (*STACKIT) ApplyReservedInstancePricing(map[string]*models.Node) {}

func (*STACKIT) GetAddresses() ([]byte, error) {
	return nil, nil
}

func (*STACKIT) GetDisks() ([]byte, error) {
	return nil, nil
}

func (*STACKIT) GetOrphanedResources() ([]models.OrphanedResource, error) {
	return nil, errors.New("not implemented")
}

func (s *STACKIT) ClusterInfo() (map[string]string, error) {
	remoteEnabled := env.IsRemoteEnabled()

	m := make(map[string]string)
	m["name"] = "STACKIT Cluster #1"
	c, err := s.GetConfig()
	if err != nil {
		return nil, err
	}
	if c.ClusterName != "" {
		m["name"] = c.ClusterName
	}
	m["provider"] = opencost.STACKITProvider
	m["region"] = s.ClusterRegion
	m["account"] = s.ClusterAccountID
	m["remoteReadEnabled"] = strconv.FormatBool(remoteEnabled)
	m["id"] = coreenv.GetClusterID()
	return m, nil
}

func (s *STACKIT) UpdateConfigFromConfigMap(a map[string]string) (*models.CustomPricing, error) {
	return s.Config.UpdateFromMap(a)
}

func (s *STACKIT) UpdateConfig(r io.Reader, updateType string) (*models.CustomPricing, error) {
	cp, err := s.Config.Update(func(c *models.CustomPricing) error {
		a := make(map[string]any)
		err := json.NewDecoder(r).Decode(&a)
		if err != nil {
			return err
		}
		for k, v := range a {
			kUpper := utils.ToTitle.String(k)
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
	if err != nil {
		return cp, err
	}

	if refreshErr := s.DownloadPricingData(); refreshErr != nil {
		log.Warnf("STACKIT: failed to refresh pricing after config update: %v", refreshErr)
	}

	return cp, nil
}

func (s *STACKIT) GetConfig() (*models.CustomPricing, error) {
	c, err := s.Config.GetCustomPricingData()
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

func (s *STACKIT) GetManagementPlatform() (string, error) {
	nodes := s.Clientset.GetAllNodes()
	if len(nodes) > 0 {
		n := nodes[0]
		if _, ok := n.Labels["node.stackit.cloud/ske"]; ok {
			return "ske", nil
		}
	}
	return "", nil
}

func (s *STACKIT) PricingSourceStatus() map[string]*models.PricingSource {
	s.DownloadPricingDataLock.RLock()
	defer s.DownloadPricingDataLock.RUnlock()
	return map[string]*models.PricingSource{
		StackitPIMPricingSource: {
			Name:      StackitPIMPricingSource,
			Enabled:   true,
			Available: len(s.pimFlavors) > 0,
		},
	}
}
