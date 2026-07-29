package huawei

import (
	"fmt"
	"io"
	"strconv"
	"sync"

	bssintlmodel "github.com/huaweicloud/huaweicloud-sdk-go-v3/services/bssintl/v2/model"
	v1 "k8s.io/api/core/v1"

	"github.com/opencost/opencost/core/pkg/clustercache"
	coreenv "github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/env"
)

// Huawei is the OpenCost provider implementation for Huawei Cloud, targeting
// clusters running on CCE (Cloud Container Engine). Node/PV pricing is fetched from
// the Huawei Cloud BSS demandPrice API (see pricingapi.go) and cached in Pricing;
// any key missing from a live query falls back to the static configs/huawei.json
// defaults, the same fallback pattern pkg/cloud/otc uses.
type Huawei struct {
	Clientset               clustercache.ClusterCache
	Config                  models.ProviderConfig
	ClusterRegion           string
	ClusterAccountID        string
	ProjectID               string
	DownloadPricingDataLock sync.RWMutex

	Pricing          map[string]*HuaweiPricing
	ValidPricingKeys map[string]bool

	BaseCPUPrice string
	BaseRAMPrice string
	BaseGPUPrice string
}

type huaweiKey struct {
	ProviderID string
	Labels     map[string]string
	VCPU       string
	RAMBytes   int64
}

func (k *huaweiKey) GPUCount() int {
	return 0
}

// TODO: Implement GPU type detection once GPU pricing is supported.
func (k *huaweiKey) GPUType() string {
	return ""
}

func (k *huaweiKey) ID() string {
	return k.ProviderID
}

func (k *huaweiKey) Features() string {
	region, _ := util.GetRegion(k.Labels)
	instanceType, _ := util.GetInstanceType(k.Labels)
	os, _ := util.GetOperatingSystem(k.Labels)
	return region + "," + instanceType + "," + os
}

// GetKey extracts/generates a key that identifies the pricing of the given node. It
// also captures the node's advertised CPU/RAM capacity from n.Status, since the BSS
// demandPrice API returns a single total instance price with no CPU/RAM split (see
// NodePricing).
func (h *Huawei) GetKey(labels map[string]string, n *clustercache.Node) models.Key {
	k := &huaweiKey{
		Labels:     labels,
		ProviderID: labels["providerID"],
	}
	if n != nil {
		if cpu, ok := n.Status.Capacity[v1.ResourceCPU]; ok {
			k.VCPU = strconv.FormatInt(cpu.Value(), 10)
		}
		if mem, ok := n.Status.Capacity[v1.ResourceMemory]; ok {
			k.RAMBytes = mem.Value()
		}
	}
	return k
}

type huaweiPVKey struct {
	Labels                 map[string]string
	StorageClassName       string
	StorageClassParameters map[string]string
	ProviderID             string
	Region                 string
}

func (k *huaweiPVKey) ID() string {
	return k.ProviderID
}

func (k *huaweiPVKey) GetStorageClass() string {
	return k.StorageClassName
}

func (k *huaweiPVKey) Features() string {
	return k.Region + "," + k.StorageClassName
}

func (h *Huawei) GetPVKey(pv *clustercache.PersistentVolume, parameters map[string]string, defaultRegion string) models.PVKey {
	return &huaweiPVKey{
		Labels:                 pv.Labels,
		StorageClassName:       pv.Spec.StorageClassName,
		StorageClassParameters: parameters,
		Region:                 defaultRegion,
	}
}

// DownloadPricingData loads the static default pricing from the huawei.json config
// (used as a fallback for any key a live query can't price), then queries the
// Huawei Cloud BSS demandPrice API for every distinct node instance-type/OS/region
// and PV storage-class/region combination currently present in the cluster.
//
// If the live query fails (e.g. missing credentials), DownloadPricingData logs the
// error and returns nil rather than failing outright: NodePricing/PVPricing already
// fall back to the static base CPU/RAM/storage prices for any key with no live
// pricing data, the same graceful-degradation behavior pkg/cloud/otc uses.
func (h *Huawei) DownloadPricingData() error {
	h.DownloadPricingDataLock.Lock()
	defer h.DownloadPricingDataLock.Unlock()

	c, err := h.Config.GetCustomPricingData()
	if err != nil {
		return err
	}

	h.BaseCPUPrice = c.CPU
	h.BaseRAMPrice = c.RAM
	h.BaseGPUPrice = c.GPU
	h.ProjectID = c.ProjectID
	if projectID := env.GetHuaweiProjectID(); projectID != "" {
		// HUAWEICLOUD_PROJECT_ID takes precedence over the static config file: the
		// project ID is region-specific (see pricingapi.go's bssRegionID comment)
		// and the BSS demandPrice API rejects requests with an empty project_id, so
		// this must be set correctly for live pricing to work at all.
		h.ProjectID = projectID
	}

	h.Pricing = make(map[string]*HuaweiPricing)
	h.ValidPricingKeys = make(map[string]bool)

	if h.Clientset == nil {
		return nil
	}

	type nodeCombo struct {
		region, instanceType, os string
	}
	nodeCombos := map[string]nodeCombo{}
	for _, node := range h.Clientset.GetAllNodes() {
		key := h.GetKey(node.Labels, node)
		region, _ := util.GetRegion(node.Labels)
		instanceType, _ := util.GetInstanceType(node.Labels)
		if region == "" || instanceType == "" {
			continue
		}
		os, _ := util.GetOperatingSystem(node.Labels)
		nodeCombos[key.Features()] = nodeCombo{region: region, instanceType: instanceType, os: os}
		h.ValidPricingKeys[key.Features()] = true
	}

	// Map each StorageClass to the Huawei EVS volume type code (SATA/SAS/GPSSD/SSD)
	// its CSI driver requests, via the "everest.io/disk-volume-type" parameter
	// (Huawei's CCE "everest" CSI driver). The StorageClass *name* (e.g.
	// "csi-disk-dss") is a cluster-specific Kubernetes name, not a BSS product
	// code, and querying demandPrice with it fails with "Product not found".
	storageClassEVSType := map[string]string{}
	for _, sc := range h.Clientset.GetAllStorageClasses() {
		if t, ok := sc.Parameters[everestDiskVolumeTypeParam]; ok && t != "" {
			storageClassEVSType[sc.Name] = t
		}
	}

	type pvCombo struct {
		region, storageClass, evsType string
	}
	pvCombos := map[string]pvCombo{}
	for _, pv := range h.Clientset.GetAllPersistentVolumes() {
		if pv.Spec.StorageClassName == "" {
			continue
		}
		evsType, ok := storageClassEVSType[pv.Spec.StorageClassName]
		if !ok {
			// Can't confidently map this storage class to a Huawei EVS product code;
			// skip the live query and let PVPricing fall back to the static default.
			log.Debugf("Huawei Cloud: no everest.io/disk-volume-type found for storage class %q, skipping live pricing for it", pv.Spec.StorageClassName)
			continue
		}
		key := h.GetPVKey(pv, map[string]string{}, h.ClusterRegion)
		pvCombos[key.Features()] = pvCombo{region: h.ClusterRegion, storageClass: pv.Spec.StorageClassName, evsType: evsType}
		h.ValidPricingKeys[key.Features()] = true
	}

	if len(nodeCombos) == 0 && len(pvCombos) == 0 {
		return nil
	}

	products := make([]bssintlmodel.DemandProductInfo, 0, len(nodeCombos)+len(pvCombos))
	idToFeatures := make(map[string]string, len(nodeCombos)+len(pvCombos))

	i := 0
	for features, combo := range nodeCombos {
		id := fmt.Sprintf("node-%d", i)
		products = append(products, buildNodeProductInfo(id, combo.region, combo.instanceType, combo.os))
		idToFeatures[id] = features
		i++
	}
	for features, combo := range pvCombos {
		id := fmt.Sprintf("pv-%d", i)
		products = append(products, buildVolumeProductInfo(id, combo.region, combo.evsType))
		idToFeatures[id] = features
		i++
	}

	ratings, err := fetchOnDemandRatings(h.ProjectID, products)
	if err != nil {
		log.Errorf("Failed to fetch Huawei Cloud BSS pricing data, falling back to static defaults: %v", err)
		return nil
	}
	if ratings.ProductRatingResults == nil {
		log.Warnf("Huawei Cloud BSS demandPrice returned no pricing results")
		return nil
	}

	for _, result := range *ratings.ProductRatingResults {
		if result.Id == nil || result.Amount == nil {
			continue
		}
		features, ok := idToFeatures[*result.Id]
		if !ok {
			continue
		}
		if combo, ok := nodeCombos[features]; ok {
			h.Pricing[features] = &HuaweiPricing{
				NodeAttributes: &HuaweiNodeAttributes{
					Type:  combo.instanceType,
					OS:    combo.os,
					Price: result.Amount.String(),
				},
			}
		} else if combo, ok := pvCombos[features]; ok {
			perGBHour := result.Amount.Div(evsReferenceSizeGBDecimal)
			h.Pricing[features] = &HuaweiPricing{
				PVAttributes: &HuaweiPVAttributes{
					Type:  combo.storageClass,
					Price: perGBHour.String(),
				},
			}
		}
	}

	return nil
}

// NodePricing returns live BSS pricing for the node's region/instance-type/OS if
// available, or the static base CPU/RAM rates otherwise. Since BSS returns a single
// total instance price with no CPU/RAM split, Node.Cost carries the live total and
// BaseCPUPrice/BaseRAMPrice remain set so the cost model can still weight the
// CPU-vs-RAM allocation split, the same pattern pkg/cloud/otc uses.
func (h *Huawei) NodePricing(key models.Key) (*models.Node, models.PricingMetadata, error) {
	h.DownloadPricingDataLock.RLock()
	defer h.DownloadPricingDataLock.RUnlock()

	meta := models.PricingMetadata{}

	hk, _ := key.(*huaweiKey)
	ramGB := ""
	vcpu := ""
	if hk != nil {
		vcpu = hk.VCPU
		if hk.RAMBytes > 0 {
			ramGB = fmt.Sprintf("%.2f", float64(hk.RAMBytes)/1024/1024/1024)
		}
	}

	pricing, ok := h.Pricing[key.Features()]
	if !ok || pricing.NodeAttributes == nil {
		return &models.Node{
			VCPU:             vcpu,
			RAM:              ramGB,
			BaseCPUPrice:     h.BaseCPUPrice,
			BaseRAMPrice:     h.BaseRAMPrice,
			BaseGPUPrice:     h.BaseGPUPrice,
			UsesBaseCPUPrice: true,
			PricingType:      models.DefaultPrices,
		}, meta, nil
	}

	return &models.Node{
		Cost:         pricing.NodeAttributes.Price,
		VCPU:         vcpu,
		RAM:          ramGB,
		BaseCPUPrice: h.BaseCPUPrice,
		BaseRAMPrice: h.BaseRAMPrice,
		BaseGPUPrice: h.BaseGPUPrice,
		InstanceType: pricing.NodeAttributes.Type,
		PricingType:  models.Api,
	}, meta, nil
}

// PVPricing returns the live BSS GB-hour price for the volume's storage class if
// available, or the static default storage price otherwise.
func (h *Huawei) PVPricing(pvk models.PVKey) (*models.PV, error) {
	h.DownloadPricingDataLock.RLock()
	defer h.DownloadPricingDataLock.RUnlock()

	if pricing, ok := h.Pricing[pvk.Features()]; ok && pricing.PVAttributes != nil {
		return &models.PV{
			Cost:  pricing.PVAttributes.Price,
			Class: pvk.GetStorageClass(),
		}, nil
	}

	c, err := h.Config.GetCustomPricingData()
	if err != nil {
		return nil, err
	}
	return &models.PV{
		Cost:  c.Storage,
		Class: pvk.GetStorageClass(),
	}, nil
}

func (h *Huawei) NetworkPricing() (*models.Network, error) {
	c, err := h.Config.GetCustomPricingData()
	if err != nil {
		return nil, err
	}
	znec, err := strconv.ParseFloat(c.ZoneNetworkEgress, 64)
	if err != nil {
		return nil, err
	}
	rnec, err := strconv.ParseFloat(c.RegionNetworkEgress, 64)
	if err != nil {
		return nil, err
	}
	inec, err := strconv.ParseFloat(c.InternetNetworkEgress, 64)
	if err != nil {
		return nil, err
	}
	nge, err := strconv.ParseFloat(c.NatGatewayEgress, 64)
	if err != nil {
		return nil, err
	}
	ngi, err := strconv.ParseFloat(c.NatGatewayIngress, 64)
	if err != nil {
		return nil, err
	}

	return &models.Network{
		ZoneNetworkEgressCost:     znec,
		RegionNetworkEgressCost:   rnec,
		InternetNetworkEgressCost: inec,
		NatGatewayEgressCost:      nge,
		NatGatewayIngressCost:     ngi,
	}, nil
}

// LoadBalancerPricing returns a static default price for a Huawei Cloud ELB.
// TODO: replace with live pricing from the Huawei Cloud BSS demandPrice API.
func (h *Huawei) LoadBalancerPricing() (*models.LoadBalancer, error) {
	return &models.LoadBalancer{
		Cost: 0.03,
	}, nil
}

func (h *Huawei) GetConfig() (*models.CustomPricing, error) {
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
		c.CurrencyCode = "CNY"
	}
	return c, nil
}

func (h *Huawei) ClusterInfo() (map[string]string, error) {
	c, err := h.GetConfig()
	if err != nil {
		return nil, err
	}
	m := make(map[string]string)
	m["name"] = "Huawei Cloud Cluster #1"
	if c.ClusterName != "" {
		m["name"] = c.ClusterName
	}
	m["provider"] = opencost.HuaweiProvider
	m["account"] = h.ClusterAccountID
	m["region"] = h.ClusterRegion
	m["remoteReadEnabled"] = strconv.FormatBool(env.IsRemoteEnabled())
	m["id"] = coreenv.GetClusterID()
	return m, nil
}

// TODO: Implement method
func (h *Huawei) UpdateConfigFromConfigMap(cm map[string]string) (*models.CustomPricing, error) {
	return &models.CustomPricing{}, nil
}

// TODO: Implement method
func (h *Huawei) UpdateConfig(r io.Reader, updateType string) (*models.CustomPricing, error) {
	return &models.CustomPricing{}, nil
}

// TODO: Implement method
func (h *Huawei) GetAddresses() ([]byte, error) {
	return []byte{}, nil
}

// TODO: Implement method
func (h *Huawei) GetDisks() ([]byte, error) {
	return []byte{}, nil
}

// TODO: Implement method
func (h *Huawei) GetOrphanedResources() ([]models.OrphanedResource, error) {
	return []models.OrphanedResource{}, nil
}

// TODO: Implement method
func (h *Huawei) GpuPricing(nodeLabels map[string]string) (string, error) {
	return "", nil
}

func (h *Huawei) AllNodePricing() (interface{}, error) {
	h.DownloadPricingDataLock.RLock()
	defer h.DownloadPricingDataLock.RUnlock()
	return h.Pricing, nil
}

// TODO: Implement method
func (h *Huawei) GetManagementPlatform() (string, error) {
	return "", nil
}

// TODO: Implement method
func (h *Huawei) ApplyReservedInstancePricing(nodes map[string]*models.Node) {}

func (h *Huawei) ServiceAccountStatus() *models.ServiceAccountStatus {
	return &models.ServiceAccountStatus{
		Checks: []*models.ServiceAccountCheck{},
	}
}

const bssPricingSourceName = "Huawei Cloud BSS demandPrice"

func (h *Huawei) PricingSourceStatus() map[string]*models.PricingSource {
	h.DownloadPricingDataLock.RLock()
	defer h.DownloadPricingDataLock.RUnlock()

	return map[string]*models.PricingSource{
		bssPricingSourceName: {
			Name:      bssPricingSourceName,
			Enabled:   true,
			Available: len(h.Pricing) > 0,
		},
	}
}

// TODO: Implement method
func (h *Huawei) ClusterManagementPricing() (string, float64, error) {
	return "", 0.0, nil
}

func (h *Huawei) CombinedDiscountForNode(instanceType string, isReserved bool, defaultDiscount, negotiatedDiscount float64) float64 {
	return 1.0 - ((1.0 - defaultDiscount) * (1.0 - negotiatedDiscount))
}

// huaweiRegions lists the published Huawei Cloud region IDs.
var huaweiRegions = []string{
	"cn-north-4",
	"cn-north-1",
	"cn-east-3",
	"cn-east-2",
	"cn-south-1",
	"cn-southwest-2",
	"ap-southeast-1",
	"ap-southeast-3",
	"af-south-1",
	"la-south-2",
	"la-north-2",
	"sa-brazil-1",
	"na-mexico-1",
	"ru-northwest-2",
	"eu-west-101",
}

func (h *Huawei) Regions() []string {
	regionOverrides := env.GetRegionOverrideList()
	if len(regionOverrides) > 0 {
		log.Debugf("Overriding Huawei Cloud regions with configured region list: %+v", regionOverrides)
		return regionOverrides
	}
	return huaweiRegions
}

// PricingSourceSummary returns the pricing source summary for the provider. The
// summary represents what was _parsed_ from the pricing source, not what was
// returned from the relevant API.
func (h *Huawei) PricingSourceSummary() interface{} {
	h.DownloadPricingDataLock.RLock()
	defer h.DownloadPricingDataLock.RUnlock()
	return h.Pricing
}
