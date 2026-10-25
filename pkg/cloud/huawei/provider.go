package huawei

import (
	"fmt"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	bssintlmodel "github.com/huaweicloud/huaweicloud-sdk-go-v3/services/bssintl/v2/model"
	v1 "k8s.io/api/core/v1"

	"github.com/opencost/opencost/core/pkg/clustercache"
	coreenv "github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/cloud/utils"
	"github.com/opencost/opencost/pkg/env"
)

// Huawei is the OpenCost provider implementation for Huawei Cloud, targeting
// clusters running on CCE (Cloud Container Engine). Node/PV pricing is fetched from
// the Huawei Cloud BSS demandPrice API (see pricingapi.go) and cached in Pricing;
// any key missing from a live query falls back to the static configs/huawei.json
// defaults, the same fallback pattern pkg/cloud/otc uses.
type Huawei struct {
	Clientset        clustercache.ClusterCache
	Config           models.ProviderConfig
	ClusterRegion    string
	ClusterAccountID string
	ProjectID        string

	// DownloadPricingDataLock guards the fields below it, which pricing
	// lookups read. DownloadPricingData only holds it to publish what it has
	// downloaded, never while it talks to the metadata service or BSS.
	DownloadPricingDataLock sync.RWMutex

	Pricing          map[string]*HuaweiPricing
	ValidPricingKeys map[string]bool

	// configGeneration counts the config updates RefreshCustomPricing has
	// applied, so a download can tell whether the config changed while it
	// was talking to BSS (see DownloadPricingData).
	configGeneration uint64

	// downloads serializes DownloadPricingData, so a slower download cannot
	// publish over a newer one. Pricing lookups never wait on it.
	downloads sync.Mutex

	// lastPricingRefresh is the UnixNano of the last cache-miss refresh,
	// used to rate-limit them (see refreshPricingInBackground).
	lastPricingRefresh atomic.Int64

	// refreshes tracks background refreshes, so tests can wait for them.
	refreshes sync.WaitGroup

	// bills caches what BSS billed for the CCE cluster, which is how
	// ClusterManagementPricing prices it (see billedcosts.go).
	bills billCache

	BaseCPUPrice string
	BaseRAMPrice string
	BaseGPUPrice string
}

type huaweiKey struct {
	ProviderID string
	Labels     map[string]string
	VCPU       string
	RAMBytes   int64
	GPUs       int
}

// gpuResourceName is the extended resource the CCE GPU add-on (gpu-beta /
// nvidia device plugin) advertises GPUs as.
const gpuResourceName v1.ResourceName = "nvidia.com/gpu"

// gpuTypeLabels are the node labels that name a GPU model, in order of
// preference: CCE's own label, then the one NVIDIA GPU Feature Discovery sets.
var gpuTypeLabels = []string{"accelerator", "nvidia.com/gpu.product"}

// GPUCount returns the GPUs the node advertises. The flavor's BSS price
// already includes them (a GPU flavor is priced as a whole instance); the
// count is what lets the cost model attribute part of that price to the GPUs
// instead of splitting it all between CPU and RAM.
func (k *huaweiKey) GPUCount() int {
	return k.GPUs
}

// GPUType returns the GPU model from the node labels, or "" when unlabeled.
func (k *huaweiKey) GPUType() string {
	if k.GPUs == 0 {
		return ""
	}
	for _, label := range gpuTypeLabels {
		if t := k.Labels[label]; t != "" {
			return t
		}
	}
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
		if gpus, ok := n.Status.Capacity[gpuResourceName]; ok {
			k.GPUs = int(gpus.Value())
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
	k := &huaweiPVKey{
		Labels:                 pv.Labels,
		StorageClassName:       pv.Spec.StorageClassName,
		StorageClassParameters: parameters,
		Region:                 defaultRegion,
	}
	// The everest CSI driver's volume handle is the EVS volume ID, which is
	// how the bill identifies the disk.
	if pv.Spec.CSI != nil {
		k.ProviderID = pv.Spec.CSI.VolumeHandle
	}
	return k
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
// resolveProviderProjectID picks the project ID used for live BSS pricing.
// HUAWEICLOUD_PROJECT_ID takes precedence over the static config file: the
// project ID is region-specific (see bssRegion in pricingapi.go) and the BSS
// demandPrice API rejects requests with an empty project_id, so it must be set
// correctly for live pricing to work at all. With neither set, it falls back to
// the instance metadata service -- the same mechanism huaweiGlobalCredentials
// (pricingapi.go) already uses for AK/SK -- so a node with an IAM agency
// attached needs no project ID provisioned anywhere.
func resolveProviderProjectID(configProjectID string) string {
	if projectID := env.GetHuaweiProjectID(); projectID != "" {
		return projectID
	}
	if configProjectID != "" {
		return configProjectID
	}
	projectID, err := huaweiProjectIDFromMetadata()
	if err != nil {
		log.Warnf("huawei cloud: %s not set and IAM agency metadata lookup failed, live BSS pricing will fail: %v", env.HuaweiProjectIDEnvVar, err)
		return ""
	}
	return projectID
}

func (h *Huawei) DownloadPricingData() error {
	h.downloads.Lock()
	defer h.downloads.Unlock()

	h.DownloadPricingDataLock.RLock()
	generation := h.configGeneration
	h.DownloadPricingDataLock.RUnlock()

	c, err := h.Config.GetCustomPricingData()
	if err != nil {
		return err
	}

	// The metadata lookup, the cluster discovery and the BSS requests all run
	// without DownloadPricingDataLock: they can take seconds, and pricing
	// lookups keep answering from the current prices meanwhile.
	projectID := resolveProviderProjectID(c.ProjectID)
	live, validKeys, requested := h.fetchLivePricing(projectID)

	h.DownloadPricingDataLock.Lock()
	defer h.DownloadPricingDataLock.Unlock()

	// RefreshCustomPricing may have applied a config update meanwhile. Its
	// base rates are then the current ones, not those read above; and live
	// prices fetched for a project the config has since moved away from are
	// dropped (the previous prices stay until a download for the new one).
	if h.configGeneration != generation {
		if current, err := h.Config.GetCustomPricingData(); err == nil {
			c = current
		}
		if h.ProjectID != "" && h.ProjectID != projectID {
			log.Infof("huawei cloud: the project ID changed during a pricing download, dropping its results for %s", projectID)
			projectID, live = h.ProjectID, nil
		}
	}

	h.BaseCPUPrice = c.CPU
	h.BaseRAMPrice = c.RAM
	h.BaseGPUPrice = c.GPU
	h.ProjectID = projectID
	h.ValidPricingKeys = validKeys
	if h.Pricing == nil {
		h.Pricing = make(map[string]*HuaweiPricing)
	}

	// The live prices are only replaced once BSS has answered with something
	// usable: a refresh that fails (e.g. a transient BSS error while pricing a
	// new node pool) keeps the previous prices rather than dropping every
	// node, disk and load balancer to the static rates until the next refresh.
	if live == nil {
		return nil
	}
	// Likewise for a partial response: a product it left unpriced keeps the
	// live price it had, rather than dropping to the static rates.
	for _, features := range requested {
		if _, priced := live[features]; priced {
			continue
		}
		if previous, ok := h.Pricing[features]; ok {
			live[features] = previous
		}
	}
	h.Pricing = live
	return nil
}

// fetchLivePricing queries BSS demandPrice for every node flavor, storage class
// and the load balancer currently in the cluster. It returns the live prices
// (nil when BSS gave nothing usable), the pricing keys of what the cluster
// holds, and the keys it asked BSS to price. It touches no shared state.
func (h *Huawei) fetchLivePricing(projectID string) (map[string]*HuaweiPricing, map[string]bool, []string) {
	validKeys := map[string]bool{}
	if h.Clientset == nil {
		return nil, validKeys, nil
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
		validKeys[key.Features()] = true
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
		validKeys[key.Features()] = true
	}

	// The cluster's Shared (Basic) ELB on-demand price is queried once per region,
	// not per in-cluster resource: Huawei Cloud ELBs aren't Node/PV resources the
	// clustercache enumerates, so there's no per-instance combo to build here.
	lbCombos := map[string]string{} // features -> region
	if h.ClusterRegion != "" {
		lbCombos[loadBalancerFeatures(h.ClusterRegion)] = h.ClusterRegion
	}

	if len(nodeCombos) == 0 && len(pvCombos) == 0 && len(lbCombos) == 0 {
		return nil, validKeys, nil
	}

	products := make([]bssintlmodel.DemandProductInfo, 0, len(nodeCombos)+len(pvCombos)+len(lbCombos))
	idToFeatures := make(map[string]string, len(nodeCombos)+len(pvCombos)+len(lbCombos))
	requested := make([]string, 0, len(nodeCombos)+len(pvCombos)+len(lbCombos))

	i := 0
	for features, combo := range nodeCombos {
		id := fmt.Sprintf("node-%d", i)
		products = append(products, buildNodeProductInfo(id, combo.region, combo.instanceType, combo.os))
		idToFeatures[id] = features
		requested = append(requested, features)
		i++
	}
	for features, combo := range pvCombos {
		id := fmt.Sprintf("pv-%d", i)
		products = append(products, buildVolumeProductInfo(id, combo.region, combo.evsType))
		idToFeatures[id] = features
		requested = append(requested, features)
		i++
	}
	for features, region := range lbCombos {
		id := fmt.Sprintf("lb-%d", i)
		products = append(products, buildLoadBalancerProductInfo(id, region))
		idToFeatures[id] = features
		requested = append(requested, features)
		i++
	}

	// A product BSS cannot price is isolated from the batch rather than
	// failing it (see fetchOnDemandRatingResults); an authentication or
	// transient failure still fails the whole download.
	results, err := fetchOnDemandRatingResults(projectID, products)
	if err != nil {
		log.Errorf("Failed to fetch Huawei Cloud BSS pricing data, keeping the previous prices (static defaults where there are none): %v", err)
		return nil, validKeys, requested
	}

	pricing := make(map[string]*HuaweiPricing, len(results))
	for _, result := range results {
		if result.Id == nil || result.Amount == nil {
			continue
		}
		features, ok := idToFeatures[*result.Id]
		if !ok {
			continue
		}
		if combo, ok := nodeCombos[features]; ok {
			pricing[features] = &HuaweiPricing{
				NodeAttributes: &HuaweiNodeAttributes{
					Type:  combo.instanceType,
					OS:    combo.os,
					Price: result.Amount.String(),
				},
			}
		} else if combo, ok := pvCombos[features]; ok {
			perGBHour := result.Amount.Div(evsReferenceSizeGBDecimal)
			pricing[features] = &HuaweiPricing{
				PVAttributes: &HuaweiPVAttributes{
					Type:  combo.storageClass,
					Price: perGBHour.String(),
				},
			}
		} else if _, ok := lbCombos[features]; ok {
			pricing[features] = &HuaweiPricing{
				LoadBalancerAttributes: &HuaweiLoadBalancerAttributes{
					Price: result.Amount.String(),
				},
			}
		}
	}
	// A response with nothing usable -- no results, an empty list, or only
	// entries missing an id or amount -- is no better than a failed request.
	if len(pricing) == 0 {
		log.Warnf("Huawei Cloud BSS demandPrice returned no usable pricing results, keeping the previous prices")
		return nil, validKeys, requested
	}
	return pricing, validKeys, requested
}

// RefreshCustomPricing reloads the base prices and project ID from config,
// skipping the cluster discovery and live BSS queries DownloadPricingData does.
// The project ID is only replaced when the env var or config sets one: the
// metadata fallback in resolveProviderProjectID is a network call.
//
// When the project ID changes, the live prices in the cache belong to the
// previous project, and lookups only refresh them on a cache miss, so a fully
// cached cluster would keep them: a download for the new project is started
// in the background instead.
func (h *Huawei) RefreshCustomPricing() error {
	projectChanged, err := h.reloadCustomPricing()
	if err != nil {
		return err
	}
	if projectChanged {
		h.startPricingRefresh()
	}
	return nil
}

// reloadCustomPricing applies the config to the provider for
// RefreshCustomPricing and reports whether the project ID changed.
func (h *Huawei) reloadCustomPricing() (bool, error) {
	h.DownloadPricingDataLock.Lock()
	defer h.DownloadPricingDataLock.Unlock()
	c, err := h.Config.GetCustomPricingData()
	if err != nil {
		return false, err
	}
	h.configGeneration++
	h.BaseCPUPrice = c.CPU
	h.BaseRAMPrice = c.RAM
	h.BaseGPUPrice = c.GPU
	previous := h.ProjectID
	if projectID := env.GetHuaweiProjectID(); projectID != "" {
		h.ProjectID = projectID
	} else if c.ProjectID != "" {
		h.ProjectID = c.ProjectID
	}
	return h.ProjectID != previous, nil
}

// pricingRefreshCooldown bounds how often a NodePricing cache miss may trigger a
// live BSS re-query. NodePricing is called once per node per cost-model cycle, so
// a flavor BSS genuinely cannot price (an unsupported spec, a revoked permission)
// would otherwise hammer the demandPrice API on every node of every cycle.
const pricingRefreshCooldown = 10 * time.Minute

// refreshPricingInBackground starts DownloadPricingData in the background to
// pick up node flavors and storage classes that appeared after the last
// download, unless one started within pricingRefreshCooldown. The lookup that
// triggered it is not held up: it answers from the current or static prices,
// and lookups after the refresh lands see the new ones. Pricing lookups run on
// the cost model's metrics path, which must not wait on BSS.
func (h *Huawei) refreshPricingInBackground() {
	if h.Config == nil {
		// Config is a required dependency for DownloadPricingData; a Huawei
		// value without one (e.g. not yet fully constructed) should still
		// degrade to base pricing rather than panic on a nil interface call.
		return
	}

	last := h.lastPricingRefresh.Load()
	now := time.Now().UnixNano()
	if last != 0 && time.Duration(now-last) < pricingRefreshCooldown {
		return
	}
	// CompareAndSwap so concurrent missing-key lookups collapse into one refresh.
	if !h.lastPricingRefresh.CompareAndSwap(last, now) {
		return
	}
	h.downloadInBackground("after a pricing cache miss")
}

// startPricingRefresh starts DownloadPricingData in the background regardless
// of pricingRefreshCooldown, for a refresh that must not be skipped. It also
// restarts the cooldown, since the download it starts covers the cache misses
// that would have triggered one.
func (h *Huawei) startPricingRefresh() {
	if h.Config == nil {
		return
	}
	h.lastPricingRefresh.Store(time.Now().UnixNano())
	h.downloadInBackground("after a project ID change")
}

// downloadInBackground runs DownloadPricingData in a goroutine tracked by
// h.refreshes. Downloads run one at a time (h.downloads), so one started while
// another is in flight runs once that one ends rather than being dropped.
func (h *Huawei) downloadInBackground(reason string) {
	h.refreshes.Add(1)
	go func() {
		defer h.refreshes.Done()
		if err := h.DownloadPricingData(); err != nil {
			log.Warnf("huawei cloud: refreshing pricing data %s failed: %v", reason, err)
		}
	}()
}

// NodePricing returns the live BSS on-demand price for the node's flavor if
// available, or the static base CPU/RAM/GPU rates otherwise. Since BSS returns a single
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
	gpu := ""
	gpuName := ""
	if hk != nil {
		vcpu = hk.VCPU
		if hk.RAMBytes > 0 {
			ramGB = fmt.Sprintf("%.2f", float64(hk.RAMBytes)/1024/1024/1024)
		}
		if hk.GPUs > 0 {
			gpu = strconv.Itoa(hk.GPUs)
			gpuName = hk.GPUType()
		}
	}

	pricing, ok := h.Pricing[key.Features()]
	if !ok || pricing.NodeAttributes == nil {
		// Cache miss. DownloadPricingData only queries BSS for the node flavors
		// present in the cluster cache at the time it ran, so any flavor that
		// joins later -- an autoscaled GPU pool, a new node pool -- would be
		// missing from h.Pricing until the process restarts. Refresh in the
		// background (rate-limited, since NodePricing runs per node per
		// cycle); this lookup falls back to the base rates meanwhile.
		h.refreshPricingInBackground()
	}
	if !ok || pricing.NodeAttributes == nil {
		// The unit costs are set to the base rates rather than left empty: with
		// no Cost, VCPUCost or GPUCost, GetNodeCost splits a price it rebuilds
		// from the defaults across CPU/RAM/GPU, and for a GPU node that path
		// multiplies the per-GiB RAM rate by RAM bytes.
		return &models.Node{
			VCPU:             vcpu,
			RAM:              ramGB,
			GPU:              gpu,
			GPUName:          gpuName,
			VCPUCost:         h.BaseCPUPrice,
			RAMCost:          h.BaseRAMPrice,
			GPUCost:          h.BaseGPUPrice,
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
		GPU:          gpu,
		GPUName:      gpuName,
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

	pricing, ok := h.Pricing[pvk.Features()]
	if !ok || pricing.PVAttributes == nil {
		// Cache miss: DownloadPricingData only priced the storage classes that
		// had PVs when it ran, so a class first used later would otherwise stay
		// on the static default until restart. Same recovery as NodePricing.
		h.refreshPricingInBackground()
	}
	if ok && pricing.PVAttributes != nil {
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

// NetworkPricing returns the static default $/GiB network rates from
// configs/huawei.json.
//
// NatGatewayEgress/NatGatewayIngress are intentionally left at "0.0", unlike most
// other providers here (which default to $0.045/GiB, matching AWS's NAT Gateway
// data-processing charge). A real Huawei Cloud bill export confirms Huawei's NAT
// Gateway (hws.service.type.natgateway) is billed as a flat per-spec fee (e.g.
// "Small NAT Gateway" at $2.438/day, SKU natgateway_small) with no separate
// per-GB data-processing charge -- traffic routed through it is billed as ordinary
// EIP bandwidth (hws.service.type.vpc, "Fixed Bandwidth" resource), the same as any
// other EIP-routed traffic. Populating these fields with the EIP bandwidth rate
// would double-count that cost against ZoneNetworkEgress/RegionNetworkEgress/
// InternetNetworkEgress once those are populated with real data, since
// costmodel/networkcosts.go sums all of these independently. The NAT Gateway's own
// flat fee is still captured correctly via CloudCost historical billing (see
// selectHuaweiCategory's "nat gateway" match in costintegration.go).
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

// loadBalancerFeatures returns the Pricing map key for a region's Huawei Cloud
// Shared (Basic) ELB on-demand price. It is keyed by region alone (mirroring the
// region-only key pkg/cloud/aws's LoadBalancerPricing uses) since the
// models.Provider interface has no key argument for load balancer pricing yet.
func loadBalancerFeatures(region string) string {
	return region + ",LoadBalancerUsage"
}

// LoadBalancerPricing returns the live BSS on-demand price for a Shared (Basic) ELB
// in the cluster's region if available, or a static default otherwise.
func (h *Huawei) LoadBalancerPricing() (*models.LoadBalancer, error) {
	h.DownloadPricingDataLock.RLock()
	defer h.DownloadPricingDataLock.RUnlock()

	if h.ClusterRegion != "" {
		if pricing, ok := h.Pricing[loadBalancerFeatures(h.ClusterRegion)]; ok && pricing.LoadBalancerAttributes != nil {
			if cost, err := strconv.ParseFloat(pricing.LoadBalancerAttributes.Price, 64); err == nil {
				return &models.LoadBalancer{Cost: cost}, nil
			}
		}
	}

	// Fall back to the configured default (defaultLBPrice in huawei.json) when
	// live BSS pricing is unavailable or the region has no published rate.
	cfg, err := h.GetConfig()
	if err != nil {
		return nil, err
	}
	lbPrice, err := strconv.ParseFloat(cfg.DefaultLBPrice, 64)
	if err != nil {
		return nil, fmt.Errorf("huawei cloud: parsing defaultLBPrice %q: %w", cfg.DefaultLBPrice, err)
	}
	return &models.LoadBalancer{
		Cost: lbPrice,
	}, nil
}

func (h *Huawei) GetConfig() (*models.CustomPricing, error) {
	shared, err := h.Config.GetCustomPricingData()
	if err != nil {
		return nil, err
	}
	// GetCustomPricingData returns the config's cached value, which other
	// goroutines read concurrently and Update writes under the config's own
	// lock: the defaults go on a copy, never on the shared value.
	cp := *shared
	c := &cp
	if c.Discount == "" {
		c.Discount = "0%"
	}
	if c.NegotiatedDiscount == "" {
		c.NegotiatedDiscount = "0%"
	}
	if c.DefaultLBPrice == "" {
		// The confirmed on-demand price of a Shared (Basic) ELB in la-south-2 per
		// a real Huawei Cloud bill export (see pricingapi.go).
		c.DefaultLBPrice = "0.053"
	}
	if c.CurrencyCode == "" {
		// The international BSS endpoint this provider queries reports amounts in
		// USD (see bssRegion), and huawei.json's fallback rates are USD too.
		c.CurrencyCode = "USD"
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

// UpdateConfigFromConfigMap applies the pricing ConfigMap to the stored
// config and then to the provider.
func (h *Huawei) UpdateConfigFromConfigMap(cm map[string]string) (*models.CustomPricing, error) {
	return h.applyConfigUpdate(h.Config.UpdateFromMap(cm))
}

// applyConfigUpdate reloads a successfully stored config update into the
// provider. The settings pipeline only calls RefreshCustomPricing when the
// rates or currency change, so a change of project ID alone would otherwise
// neither reach h.ProjectID nor advance configGeneration, and a pricing
// download in flight would publish the previous project's prices and put its
// project ID back.
func (h *Huawei) applyConfigUpdate(c *models.CustomPricing, err error) (*models.CustomPricing, error) {
	if err != nil {
		return c, err
	}
	if err := h.RefreshCustomPricing(); err != nil {
		log.Warnf("huawei cloud: config updated but not reloaded into the provider: %v", err)
	}
	return c, nil
}

// UpdateConfig applies a JSON object of custom pricing fields to the stored
// config, the same way CustomProvider does.
func (h *Huawei) UpdateConfig(r io.Reader, updateType string) (*models.CustomPricing, error) {
	a := make(map[string]interface{})
	if err := json.NewDecoder(r).Decode(&a); err != nil {
		return nil, err
	}
	return h.applyConfigUpdate(h.Config.Update(func(c *models.CustomPricing) error {
		for k, v := range a {
			kUpper := utils.ToTitle.String(k)
			vstr, ok := v.(string)
			if !ok {
				return fmt.Errorf("type error while updating config for %s", kUpper)
			}
			if err := models.SetCustomPricingField(c, kUpper, vstr); err != nil {
				return fmt.Errorf("error setting custom pricing field: %w", err)
			}
		}
		return nil
	}))
}

// The methods below satisfy models.Provider but have no Huawei Cloud
// implementation. They return zero values, which the cost model treats as "no
// data available" rather than an error, the same way pkg/cloud/stackit does for
// the capabilities it does not support.

func (h *Huawei) GetAddresses() ([]byte, error) {
	return []byte{}, nil
}

func (h *Huawei) GetDisks() ([]byte, error) {
	return []byte{}, nil
}

func (h *Huawei) GetOrphanedResources() ([]models.OrphanedResource, error) {
	return []models.OrphanedResource{}, nil
}

func (h *Huawei) GpuPricing(nodeLabels map[string]string) (string, error) {
	return "", nil
}

func (h *Huawei) AllNodePricing() (interface{}, error) {
	h.DownloadPricingDataLock.RLock()
	defer h.DownloadPricingDataLock.RUnlock()
	return h.Pricing, nil
}

func (h *Huawei) GetManagementPlatform() (string, error) {
	return "", nil
}

func (h *Huawei) ApplyReservedInstancePricing(nodes map[string]*models.Node) {
	// Huawei Cloud reserved instances are not exposed through the BSS
	// demandPrice API this provider queries, so there is nothing to apply.
}

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
