package nebius

import (
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"sync"

	coreenv "github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/util"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/cloud/utils"
	"github.com/opencost/opencost/pkg/env"
)

const (
	NebiusConfigPricing = "Nebius Config Pricing"
)

// Known Nebius GPU platforms and their GPU model names.
var gpuPlatformModels = map[string]string{
	"gpu-h100-sxm":   "H100",
	"gpu-h200-sxm":   "H200",
	"gpu-l40s-pcie":  "L40S",
	"gpu-b200-sxm":   "B200",
	"gpu-b200-sxm-a": "B200",
	"gpu-b300-sxm":   "B300",
}

// presetPattern matches Nebius preset names like "1gpu-16vcpu-200gb" or "16vcpu-64gb".
var presetPattern = regexp.MustCompile(`^(?:(\d+)gpu-)?(\d+)vcpu-(\d+)gb$`)

// NebiusPricing holds cached pricing data for a platform/preset combination.
type NebiusPricing struct {
	PlatformID string
	PresetID   string
	VCPU       int
	RAMGB      int
	GPU        int
	GPUModel   string
	HourlyCost float64
}

// Nebius implements the models.Provider interface for Nebius AI Cloud.
type Nebius struct {
	Clientset               clustercache.ClusterCache
	Config                  models.ProviderConfig
	Pricing                 map[string]*NebiusPricing
	ClusterRegion           string
	ClusterAccountID        string
	DownloadPricingDataLock sync.RWMutex
}

// PricingSourceSummary returns the pricing source summary for the provider.
func (n *Nebius) PricingSourceSummary() interface{} {
	return n.Pricing
}

// DownloadPricingData loads pricing from config defaults. When Nebius service
// account credentials are available (NEBIUS_SA_ID, NEBIUS_SA_PUBLIC_KEY_ID,
// NEBIUS_SA_PRIVATE_KEY_PATH), future versions will use the CalculatorService
// gRPC API for live pricing. For now, costs are derived from the config defaults.
func (n *Nebius) DownloadPricingData() error {
	n.DownloadPricingDataLock.Lock()
	defer n.DownloadPricingDataLock.Unlock()

	if n.Pricing != nil {
		return nil
	}

	n.Pricing = make(map[string]*NebiusPricing)

	c, err := n.GetConfig()
	if err != nil {
		return fmt.Errorf("failed to get Nebius config: %w", err)
	}

	log.Infof("Nebius: loaded default pricing from config (CPU=%s, RAM=%s, GPU=%s)", c.CPU, c.RAM, c.GPU)

	// Log whether service account credentials are configured for future
	// CalculatorService API integration.
	if env.GetNebiusServiceAccountID() != "" {
		log.Infof("Nebius: service account credentials detected. " +
			"CalculatorService API integration is planned for a future release.")
	}

	return nil
}

// AllNodePricing returns all cached node pricing.
func (n *Nebius) AllNodePricing() (interface{}, error) {
	n.DownloadPricingDataLock.RLock()
	defer n.DownloadPricingDataLock.RUnlock()
	return n.Pricing, nil
}

// nebiusKey implements models.Key for Nebius nodes.
type nebiusKey struct {
	Labels     map[string]string
	ProviderID string
}

// Features returns "zone,instanceType" used for pricing lookup.
func (k *nebiusKey) Features() string {
	instanceType, _ := util.GetInstanceType(k.Labels)
	zone, _ := util.GetZone(k.Labels)
	return zone + "," + instanceType
}

// GPUCount returns the number of GPUs parsed from the instance type / preset name.
// Nebius presets follow the pattern "1gpu-16vcpu-200gb".
func (k *nebiusKey) GPUCount() int {
	instanceType, _ := util.GetInstanceType(k.Labels)
	count, _, _ := parsePreset(instanceType)
	return count
}

// GPUType returns the GPU model name parsed from the platform label or instance type.
func (k *nebiusKey) GPUType() string {
	instanceType, _ := util.GetInstanceType(k.Labels)

	// Check if the instance type itself is a known GPU platform
	if model, ok := gpuPlatformModels[instanceType]; ok {
		return model
	}

	// Try to extract GPU model from the instance type prefix
	lower := strings.ToLower(instanceType)
	for platform, model := range gpuPlatformModels {
		if strings.Contains(lower, platform) {
			return model
		}
	}

	// Check for a Nebius-specific platform label
	if platform, ok := k.Labels["nebius.com/platform"]; ok {
		if model, okModel := gpuPlatformModels[platform]; okModel {
			return model
		}
	}

	return ""
}

// ID returns the provider ID.
func (k *nebiusKey) ID() string {
	return k.ProviderID
}

// NodePricing returns pricing for a node based on its key.
func (n *Nebius) NodePricing(key models.Key) (*models.Node, models.PricingMetadata, error) {
	n.DownloadPricingDataLock.RLock()
	defer n.DownloadPricingDataLock.RUnlock()

	meta := models.PricingMetadata{}

	features := key.Features()
	split := strings.Split(features, ",")

	zone := ""
	instanceType := ""
	if len(split) >= 2 {
		zone = split[0]
		instanceType = split[1]
	}

	// Check cached API pricing first
	pricingKey := instanceType
	if pricing, ok := n.Pricing[pricingKey]; ok {
		return &models.Node{
			Cost:         fmt.Sprintf("%f", pricing.HourlyCost),
			PricingType:  models.DefaultPrices,
			VCPU:         fmt.Sprintf("%d", pricing.VCPU),
			RAM:          fmt.Sprintf("%d", pricing.RAMGB),
			GPU:          fmt.Sprintf("%d", pricing.GPU),
			InstanceType: instanceType,
			Region:       zone,
			GPUName:      pricing.GPUModel,
		}, meta, nil
	}

	// Fall back to config-based pricing
	c, err := n.GetConfig()
	if err != nil {
		return nil, meta, fmt.Errorf("failed to get Nebius config: %w", err)
	}

	gpuCount, vcpuCount, ramGB := parsePreset(instanceType)
	if instanceType != "" && vcpuCount == 0 && ramGB == 0 {
		log.Warnf("Nebius: could not parse preset %q; cost will be zero", instanceType)
	}
	gpuModel := ""
	if gpuCount > 0 {
		gpuModel = key.GPUType()
	}

	cpuCost, err := strconv.ParseFloat(c.CPU, 64)
	if err != nil {
		log.Warnf("Nebius: failed to parse CPU cost %q, using 0: %v", c.CPU, err)
	}
	ramCost, err := strconv.ParseFloat(c.RAM, 64)
	if err != nil {
		log.Warnf("Nebius: failed to parse RAM cost %q, using 0: %v", c.RAM, err)
	}
	gpuCostPerUnit, err := strconv.ParseFloat(c.GPU, 64)
	if err != nil {
		log.Warnf("Nebius: failed to parse GPU cost %q, using 0: %v", c.GPU, err)
	}

	totalCost := float64(vcpuCount)*cpuCost + float64(ramGB)*ramCost + float64(gpuCount)*gpuCostPerUnit

	node := &models.Node{
		Cost:         fmt.Sprintf("%f", totalCost),
		PricingType:  models.DefaultPrices,
		VCPU:         fmt.Sprintf("%d", vcpuCount),
		RAM:          fmt.Sprintf("%d", ramGB),
		GPU:          fmt.Sprintf("%d", gpuCount),
		InstanceType: instanceType,
		Region:       zone,
		GPUName:      gpuModel,
	}

	if vcpuCount > 0 {
		node.VCPUCost = fmt.Sprintf("%f", cpuCost)
	}
	if ramGB > 0 {
		node.RAMCost = fmt.Sprintf("%f", ramCost)
	}
	if gpuCount > 0 {
		node.GPUCost = fmt.Sprintf("%f", gpuCostPerUnit)
	}

	return node, meta, nil
}

// LoadBalancerPricing returns static load balancer pricing.
func (n *Nebius) LoadBalancerPricing() (*models.LoadBalancer, error) {
	return &models.LoadBalancer{
		Cost: 0.02,
	}, nil
}

// NetworkPricing returns network pricing. Nebius currently offers free egress/ingress.
func (n *Nebius) NetworkPricing() (*models.Network, error) {
	return &models.Network{
		ZoneNetworkEgressCost:     0,
		RegionNetworkEgressCost:   0,
		InternetNetworkEgressCost: 0,
		NatGatewayEgressCost:      0,
		NatGatewayIngressCost:     0,
	}, nil
}

// GetKey returns a pricing key for the given node.
func (n *Nebius) GetKey(labels map[string]string, node *clustercache.Node) models.Key {
	return &nebiusKey{
		Labels:     labels,
		ProviderID: node.SpecProviderID,
	}
}

// nebiusPVKey implements models.PVKey for Nebius persistent volumes.
type nebiusPVKey struct {
	Labels           map[string]string
	StorageClassName string
	Zone             string
}

// ID returns an empty string as PV IDs are not used for Nebius pricing.
func (k *nebiusPVKey) ID() string {
	return ""
}

// GetStorageClass returns the storage class name.
func (k *nebiusPVKey) GetStorageClass() string {
	return k.StorageClassName
}

// Features returns the zone for PV pricing lookup.
func (k *nebiusPVKey) Features() string {
	return k.Zone
}

// GetPVKey returns a PV pricing key.
func (n *Nebius) GetPVKey(pv *clustercache.PersistentVolume, parameters map[string]string, defaultRegion string) models.PVKey {
	zone := defaultRegion
	if pv.Spec.CSI != nil {
		parts := strings.Split(pv.Spec.CSI.VolumeHandle, "/")
		if len(parts) > 0 {
			zone = parts[0]
		}
	}
	return &nebiusPVKey{
		Labels:           pv.Labels,
		StorageClassName: pv.Spec.StorageClassName,
		Zone:             zone,
	}
}

// GpuPricing returns GPU type information for a node.
func (n *Nebius) GpuPricing(nodeLabels map[string]string) (string, error) {
	return "", nil
}

// PVPricing returns persistent volume pricing.
func (n *Nebius) PVPricing(pvk models.PVKey) (*models.PV, error) {
	c, err := n.GetConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get Nebius config for PV pricing: %w", err)
	}

	storageCost := c.Storage
	if storageCost == "" {
		storageCost = "0.00005"
	}

	return &models.PV{
		Cost:  storageCost,
		Class: pvk.GetStorageClass(),
	}, nil
}

// ServiceAccountStatus returns the status of service account checks.
func (n *Nebius) ServiceAccountStatus() *models.ServiceAccountStatus {
	checks := []*models.ServiceAccountCheck{}

	saID := env.GetNebiusServiceAccountID()
	saKey := env.GetNebiusServiceAccountPublicKeyID()
	saPriv := env.GetNebiusServiceAccountPrivateKeyPath()

	if saID == "" && saKey == "" && saPriv == "" {
		checks = append(checks, &models.ServiceAccountCheck{
			Message: "Nebius service account not configured; using default pricing",
			Status:  false,
		})
	} else {
		if saID == "" {
			checks = append(checks, &models.ServiceAccountCheck{
				Message: "NEBIUS_SA_ID is not set",
				Status:  false,
			})
		}
		if saKey == "" {
			checks = append(checks, &models.ServiceAccountCheck{
				Message: "NEBIUS_SA_PUBLIC_KEY_ID is not set",
				Status:  false,
			})
		}
		if saPriv == "" {
			checks = append(checks, &models.ServiceAccountCheck{
				Message: "NEBIUS_SA_PRIVATE_KEY_PATH is not set",
				Status:  false,
			})
		}
		if saID != "" && saKey != "" && saPriv != "" {
			checks = append(checks, &models.ServiceAccountCheck{
				Message: "Nebius service account configured",
				Status:  true,
			})
		}
	}

	return &models.ServiceAccountStatus{
		Checks: checks,
	}
}

// ClusterManagementPricing returns cluster management pricing.
// Nebius managed Kubernetes control plane is free.
func (n *Nebius) ClusterManagementPricing() (string, float64, error) {
	return "", 0.0, nil
}

// CombinedDiscountForNode returns the combined discount for a node.
func (n *Nebius) CombinedDiscountForNode(instanceType string, isPreemptible bool, defaultDiscount, negotiatedDiscount float64) float64 {
	return 1.0 - ((1.0 - defaultDiscount) * (1.0 - negotiatedDiscount))
}

// Regions returns the list of available Nebius regions.
func (n *Nebius) Regions() []string {
	regionOverrides := env.GetRegionOverrideList()
	if len(regionOverrides) > 0 {
		log.Debugf("Overriding Nebius regions with configured region list: %+v", regionOverrides)
		return regionOverrides
	}

	// Known Nebius regions/zones
	return []string{
		"eu-west1",
		"eu-north1",
	}
}

// ApplyReservedInstancePricing is a no-op for Nebius.
func (n *Nebius) ApplyReservedInstancePricing(nodes map[string]*models.Node) {}

// GetAddresses returns nil as address discovery is not implemented for Nebius.
func (n *Nebius) GetAddresses() ([]byte, error) {
	return nil, nil
}

// GetDisks returns nil as disk discovery is not implemented for Nebius.
func (n *Nebius) GetDisks() ([]byte, error) {
	return nil, nil
}

// GetOrphanedResources returns an error as orphaned resource detection is not implemented.
func (n *Nebius) GetOrphanedResources() ([]models.OrphanedResource, error) {
	return nil, errors.New("not implemented")
}

// ClusterInfo returns identifying information about the cluster.
func (n *Nebius) ClusterInfo() (map[string]string, error) {
	remoteEnabled := env.IsRemoteEnabled()

	m := make(map[string]string)
	m["name"] = "Nebius Cluster #1"
	c, err := n.GetConfig()
	if err != nil {
		return nil, err
	}
	if c.ClusterName != "" {
		m["name"] = c.ClusterName
	}
	m["provider"] = opencost.NebiusProvider
	m["region"] = n.ClusterRegion
	m["account"] = n.ClusterAccountID
	m["remoteReadEnabled"] = strconv.FormatBool(remoteEnabled)
	m["id"] = coreenv.GetClusterID()
	return m, nil
}

// UpdateConfigFromConfigMap updates provider config from a Kubernetes ConfigMap.
func (n *Nebius) UpdateConfigFromConfigMap(a map[string]string) (*models.CustomPricing, error) {
	return n.Config.UpdateFromMap(a)
}

// UpdateConfig updates provider config from a reader.
func (n *Nebius) UpdateConfig(r io.Reader, updateType string) (*models.CustomPricing, error) {
	defer n.DownloadPricingData()

	return n.Config.Update(func(c *models.CustomPricing) error {
		a := make(map[string]interface{})
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
}

// GetConfig returns the provider's custom pricing configuration.
func (n *Nebius) GetConfig() (*models.CustomPricing, error) {
	c, err := n.Config.GetCustomPricingData()
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
		c.CurrencyCode = "USD"
	}
	return c, nil
}

// GetManagementPlatform returns the management platform identifier.
func (n *Nebius) GetManagementPlatform() (string, error) {
	nodes := n.Clientset.GetAllNodes()

	if len(nodes) > 0 {
		node := nodes[0]
		// Check for Nebius MK8S (Managed Kubernetes) labels
		if _, ok := node.Labels["nebius.com/mk8s"]; ok {
			return "mk8s", nil
		}
		if _, ok := node.Labels["node.kubernetes.io/instance-type"]; ok {
			providerID := strings.ToLower(node.SpecProviderID)
			if strings.HasPrefix(providerID, "nebius") {
				return "mk8s", nil
			}
		}
	}
	return "", nil
}

// PricingSourceStatus returns status information about pricing data sources.
func (n *Nebius) PricingSourceStatus() map[string]*models.PricingSource {
	return map[string]*models.PricingSource{
		NebiusConfigPricing: {
			Name:      NebiusConfigPricing,
			Enabled:   true,
			Available: true,
		},
	}
}

// parsePreset extracts GPU count, vCPU count, and RAM (GB) from a Nebius preset name.
// Nebius presets follow the pattern "1gpu-16vcpu-200gb" or "16vcpu-64gb".
// Returns (gpuCount, vcpuCount, ramGB). Returns (0, 0, 0) if the name cannot be parsed.
func parsePreset(preset string) (gpuCount int, vcpuCount int, ramGB int) {
	matches := presetPattern.FindStringSubmatch(strings.ToLower(preset))
	if matches == nil {
		return 0, 0, 0
	}

	if matches[1] != "" {
		gpuCount, _ = strconv.Atoi(matches[1])
	}
	vcpuCount, _ = strconv.Atoi(matches[2])
	ramGB, _ = strconv.Atoi(matches[3])

	return gpuCount, vcpuCount, ramGB
}
