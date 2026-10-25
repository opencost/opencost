package huawei

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/huaweicloud/huaweicloud-sdk-go-v3/core/auth"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/cloud/utils"
	"github.com/opencost/opencost/pkg/config"
	"github.com/opencost/opencost/pkg/env"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

type fakeProviderConfig struct {
	customPricing *models.CustomPricing
}

func (f *fakeProviderConfig) GetCustomPricingData() (*models.CustomPricing, error) {
	if f.customPricing != nil {
		return f.customPricing, nil
	}
	return nil, fmt.Errorf("no config")
}

func (f *fakeProviderConfig) Update(func(*models.CustomPricing) error) (*models.CustomPricing, error) {
	return nil, fmt.Errorf("no config")
}

func (f *fakeProviderConfig) UpdateFromMap(map[string]string) (*models.CustomPricing, error) {
	return nil, fmt.Errorf("no config")
}

func (f *fakeProviderConfig) ConfigFileManager() *config.ConfigFileManager { return nil }

func TestHuaweiKey_Features(t *testing.T) {
	k := &huaweiKey{
		Labels: map[string]string{
			"topology.kubernetes.io/region":    "la-south-2",
			"node.kubernetes.io/instance-type": "c7n.2xlarge.4",
			"kubernetes.io/os":                 "linux",
		},
	}
	expected := "la-south-2,c7n.2xlarge.4,linux"
	if got := k.Features(); got != expected {
		t.Fatalf("expected features %q, got %q", expected, got)
	}
	if k.GPUCount() != 0 {
		t.Fatalf("expected GPUCount 0, got %d", k.GPUCount())
	}
	if k.GPUType() != "" {
		t.Fatalf("expected empty GPUType, got %q", k.GPUType())
	}
}

func TestHuawei_GetKey(t *testing.T) {
	h := &Huawei{}
	node := &clustercache.Node{Labels: map[string]string{"providerID": "abc-123"}}
	key := h.GetKey(node.Labels, node)
	if key.ID() != "abc-123" {
		t.Fatalf("expected key ID abc-123, got %s", key.ID())
	}
}

func TestHuawei_GetPVKey(t *testing.T) {
	h := &Huawei{}
	pv := &clustercache.PersistentVolume{Spec: v1.PersistentVolumeSpec{StorageClassName: "sata"}}
	key := h.GetPVKey(pv, nil, "la-south-2")
	if key.GetStorageClass() != "sata" {
		t.Fatalf("expected storage class sata, got %s", key.GetStorageClass())
	}
	expectedFeatures := "la-south-2,sata"
	if key.Features() != expectedFeatures {
		t.Fatalf("expected features %q, got %q", expectedFeatures, key.Features())
	}
}

func TestHuawei_DownloadPricingData(t *testing.T) {
	h := &Huawei{Config: &fakeProviderConfig{customPricing: &models.CustomPricing{CPU: "0.0345", RAM: "0.0046", GPU: "0.0"}}}
	if err := h.DownloadPricingData(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h.BaseCPUPrice != "0.0345" {
		t.Fatalf("expected BaseCPUPrice 0.0345, got %s", h.BaseCPUPrice)
	}
	if h.BaseRAMPrice != "0.0046" {
		t.Fatalf("expected BaseRAMPrice 0.0046, got %s", h.BaseRAMPrice)
	}
}

func TestHuawei_DownloadPricingData_Error(t *testing.T) {
	h := &Huawei{Config: &fakeProviderConfig{}}
	if err := h.DownloadPricingData(); err == nil {
		t.Fatalf("expected error from DownloadPricingData with missing config")
	}
}

func TestHuawei_RefreshCustomPricing(t *testing.T) {
	t.Setenv(env.HuaweiProjectIDEnvVar, "")
	live := map[string]*HuaweiPricing{"r,t,linux": {}}
	h := &Huawei{
		Config:    &fakeProviderConfig{customPricing: &models.CustomPricing{CPU: "0.05", RAM: "0.006", GPU: "1.2", ProjectID: "cfg-project"}},
		ProjectID: "old-project",
		Pricing:   live,
	}
	if err := h.RefreshCustomPricing(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// The project change starts a download (with no Clientset, it queries
	// nothing); it reads h.Config, which this test replaces below.
	h.refreshes.Wait()
	if h.BaseCPUPrice != "0.05" || h.BaseRAMPrice != "0.006" || h.BaseGPUPrice != "1.2" {
		t.Fatalf("base prices not reloaded: cpu=%s ram=%s gpu=%s", h.BaseCPUPrice, h.BaseRAMPrice, h.BaseGPUPrice)
	}
	if h.ProjectID != "cfg-project" {
		t.Fatalf("expected ProjectID cfg-project, got %s", h.ProjectID)
	}
	if len(h.Pricing) != 1 {
		t.Fatalf("expected live pricing cache to be kept, got %d entries", len(h.Pricing))
	}

	// An empty config project ID keeps the current one rather than falling
	// through to the metadata lookup.
	h.Config = &fakeProviderConfig{customPricing: &models.CustomPricing{}}
	if err := h.RefreshCustomPricing(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h.ProjectID != "cfg-project" {
		t.Fatalf("expected ProjectID to stay cfg-project, got %s", h.ProjectID)
	}
}

func TestHuawei_RefreshCustomPricing_Error(t *testing.T) {
	h := &Huawei{Config: &fakeProviderConfig{}}
	if err := h.RefreshCustomPricing(); err == nil {
		t.Fatalf("expected error from RefreshCustomPricing with missing config")
	}
}

func TestHuawei_NodePricing_FallsBackToBaseWhenNoLivePricing(t *testing.T) {
	h := &Huawei{BaseCPUPrice: "0.0345", BaseRAMPrice: "0.0046", BaseGPUPrice: "0.0"}
	node, _, err := h.NodePricing(&huaweiKey{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if node.BaseCPUPrice != "0.0345" || node.BaseRAMPrice != "0.0046" {
		t.Fatalf("unexpected node pricing: %+v", node)
	}
	if !node.UsesBaseCPUPrice {
		t.Fatalf("expected UsesBaseCPUPrice to be true")
	}
	if node.Cost != "" {
		t.Fatalf("expected empty Cost when falling back, got %q", node.Cost)
	}
}

// TestHuawei_NodePricing_GPUFallbackSetsUnitCosts guards the GPU node fallback:
// with no live price, the unit costs must be the base rates. Left empty, they
// send GetNodeCost into its GPU ratio path, which multiplies the per-GiB RAM
// rate by RAM bytes and prices an 8 vCPU/32 GiB/1 GPU node in the millions.
func TestHuawei_NodePricing_GPUFallbackSetsUnitCosts(t *testing.T) {
	h := &Huawei{BaseCPUPrice: "0.018901", BaseRAMPrice: "0.0025202", BaseGPUPrice: "0.216468"}
	h.lastPricingRefresh.Store(time.Now().UnixNano())
	node, _, err := h.NodePricing(&huaweiKey{VCPU: "8", RAMBytes: 32 * 1024 * 1024 * 1024, GPUs: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if node.GPU != "1" {
		t.Fatalf("expected GPU 1, got %q", node.GPU)
	}
	if node.VCPUCost != "0.018901" || node.RAMCost != "0.0025202" || node.GPUCost != "0.216468" {
		t.Fatalf("expected base unit costs, got cpu=%q ram=%q gpu=%q", node.VCPUCost, node.RAMCost, node.GPUCost)
	}
}

func TestHuawei_NodePricing_UsesLivePricingWhenAvailable(t *testing.T) {
	h := &Huawei{
		BaseCPUPrice: "0.0345",
		BaseRAMPrice: "0.0046",
		Pricing: map[string]*HuaweiPricing{
			"la-south-2,c7n.2xlarge.4,linux": {
				NodeAttributes: &HuaweiNodeAttributes{Type: "c7n.2xlarge.4", OS: "linux", Price: "1.234"},
			},
		},
	}
	key := &huaweiKey{
		Labels: map[string]string{
			"topology.kubernetes.io/region":    "la-south-2",
			"node.kubernetes.io/instance-type": "c7n.2xlarge.4",
			"kubernetes.io/os":                 "linux",
		},
		VCPU:     "8",
		RAMBytes: 16 * 1024 * 1024 * 1024,
	}
	node, _, err := h.NodePricing(key)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if node.Cost != "1.234" {
		t.Fatalf("expected live cost 1.234, got %q", node.Cost)
	}
	if node.VCPU != "8" {
		t.Fatalf("expected VCPU 8, got %q", node.VCPU)
	}
	if node.RAM != "16.00" {
		t.Fatalf("expected RAM 16.00, got %q", node.RAM)
	}
	if node.UsesBaseCPUPrice {
		t.Fatalf("expected UsesBaseCPUPrice to be false when live pricing is used")
	}
}

func TestHuawei_PVPricing_FallsBackToStaticDefault(t *testing.T) {
	h := &Huawei{Config: &fakeProviderConfig{customPricing: &models.CustomPricing{Storage: "0.00007"}}}
	// The cache miss starts a background pricing refresh; wait for it so it
	// cannot outlive this test and read package globals (metadataEndpoint,
	// bssEndpointOverride) while a later test replaces them.
	defer h.refreshes.Wait()
	pv, err := h.PVPricing(&huaweiPVKey{StorageClassName: "sata"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pv.Cost != "0.00007" || pv.Class != "sata" {
		t.Fatalf("unexpected PV pricing: %+v", pv)
	}
}

func TestHuawei_PVPricing_UsesLivePricingWhenAvailable(t *testing.T) {
	h := &Huawei{
		Pricing: map[string]*HuaweiPricing{
			"la-south-2,sata": {PVAttributes: &HuaweiPVAttributes{Type: "sata", Price: "0.0001"}},
		},
	}
	pv, err := h.PVPricing(&huaweiPVKey{Region: "la-south-2", StorageClassName: "sata"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pv.Cost != "0.0001" {
		t.Fatalf("expected live cost 0.0001, got %q", pv.Cost)
	}
}

func TestHuawei_NetworkPricing(t *testing.T) {
	h := &Huawei{Config: &fakeProviderConfig{customPricing: &models.CustomPricing{
		ZoneNetworkEgress:     "0.0",
		RegionNetworkEgress:   "0.0",
		InternetNetworkEgress: "0.0",
		NatGatewayEgress:      "0.0",
		NatGatewayIngress:     "0.0",
	}}}
	if _, err := h.NetworkPricing(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestHuawei_NetworkPricing_Error(t *testing.T) {
	h := &Huawei{Config: &fakeProviderConfig{}}
	if _, err := h.NetworkPricing(); err == nil {
		t.Fatalf("expected error for missing config")
	}
}

func TestHuawei_ClusterInfo(t *testing.T) {
	h := &Huawei{
		Config:           &fakeProviderConfig{customPricing: &models.CustomPricing{}},
		ClusterRegion:    "la-south-2",
		ClusterAccountID: "acct-1",
	}
	info, err := h.ClusterInfo()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if info["provider"] != "Huawei" {
		t.Fatalf("expected provider Huawei, got %s", info["provider"])
	}
	if info["region"] != "la-south-2" {
		t.Fatalf("expected region la-south-2, got %s", info["region"])
	}
}

func TestHuawei_ClusterInfo_Error(t *testing.T) {
	h := &Huawei{Config: &fakeProviderConfig{}}
	if _, err := h.ClusterInfo(); err == nil {
		t.Fatalf("expected error for missing config")
	}
}

func TestHuawei_Regions(t *testing.T) {
	h := &Huawei{}
	regions := h.Regions()
	if len(regions) == 0 {
		t.Fatalf("expected non-empty region list")
	}
}

func TestHuawei_CombinedDiscountForNode(t *testing.T) {
	h := &Huawei{}
	discount := h.CombinedDiscountForNode("c7n.2xlarge.4", false, 0.1, 0.1)
	expected := 1.0 - ((1.0 - 0.1) * (1.0 - 0.1))
	if diff := discount - expected; diff > 1e-9 || diff < -1e-9 {
		t.Fatalf("expected discount %v, got %v", expected, discount)
	}
}

func TestHuawei_ServiceAccountStatus(t *testing.T) {
	h := &Huawei{}
	if status := h.ServiceAccountStatus(); status == nil {
		t.Fatalf("expected non-nil ServiceAccountStatus")
	}
}

func TestHuawei_PricingSourceStatus(t *testing.T) {
	h := &Huawei{}
	status := h.PricingSourceStatus()
	entry, ok := status[bssPricingSourceName]
	if !ok {
		t.Fatalf("expected %q entry, got %+v", bssPricingSourceName, status)
	}
	if entry.Available {
		t.Fatalf("expected Available false with no pricing data loaded")
	}

	h.Pricing = map[string]*HuaweiPricing{"k": {}}
	entry = h.PricingSourceStatus()[bssPricingSourceName]
	if !entry.Available {
		t.Fatalf("expected Available true once pricing data is loaded")
	}
}

func TestHuawei_LoadBalancerPricing(t *testing.T) {
	// No live pricing: the price comes from defaultLBPrice in the provider config.
	h := &Huawei{Config: &fakeProviderConfig{customPricing: &models.CustomPricing{DefaultLBPrice: "0.071"}}}
	lb, err := h.LoadBalancerPricing()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if lb.Cost != 0.071 {
		t.Fatalf("expected configured fallback cost 0.071, got %v", lb.Cost)
	}
}

// TestHuawei_LoadBalancerPricingDefault covers the built-in fallback applied by
// GetConfig when defaultLBPrice is absent from the config.
func TestHuawei_LoadBalancerPricingDefault(t *testing.T) {
	h := &Huawei{Config: &fakeProviderConfig{customPricing: &models.CustomPricing{}}}
	lb, err := h.LoadBalancerPricing()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if lb.Cost != 0.053 {
		t.Fatalf("expected built-in fallback cost 0.053, got %v", lb.Cost)
	}
}

func TestHuawei_LoadBalancerPricing_UsesLivePricingWhenAvailable(t *testing.T) {
	h := &Huawei{
		ClusterRegion: "la-south-2",
		Pricing: map[string]*HuaweiPricing{
			"la-south-2,LoadBalancerUsage": {LoadBalancerAttributes: &HuaweiLoadBalancerAttributes{Price: "0.053"}},
		},
	}
	lb, err := h.LoadBalancerPricing()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if lb.Cost != 0.053 {
		t.Fatalf("expected live cost 0.053, got %v", lb.Cost)
	}
}

func TestHuawei_UnimplementedStubs(t *testing.T) {
	h := &Huawei{}
	if _, err := h.GetAddresses(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := h.GetDisks(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := h.GetOrphanedResources(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := h.GpuPricing(nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := h.AllNodePricing(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := h.GetManagementPlatform(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, _, err := h.ClusterManagementPricing(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	h.ApplyReservedInstancePricing(map[string]*models.Node{})
}

// updatableProviderConfig is a ProviderConfig whose updates apply to an
// in-memory CustomPricing, or fail with err when it is set.
type updatableProviderConfig struct {
	fakeProviderConfig
	err error
}

func (u *updatableProviderConfig) Update(update func(*models.CustomPricing) error) (*models.CustomPricing, error) {
	if u.err != nil {
		return nil, u.err
	}
	if err := update(u.customPricing); err != nil {
		return nil, err
	}
	return u.customPricing, nil
}

func (u *updatableProviderConfig) UpdateFromMap(cm map[string]string) (*models.CustomPricing, error) {
	return u.Update(func(c *models.CustomPricing) error {
		for k, v := range cm {
			if err := models.SetCustomPricingField(c, k, v); err != nil {
				return err
			}
		}
		return nil
	})
}

func TestHuawei_UpdateConfigFromConfigMap(t *testing.T) {
	cfg := &updatableProviderConfig{fakeProviderConfig: fakeProviderConfig{customPricing: &models.CustomPricing{CPU: "0.018901"}}}
	h := &Huawei{Config: cfg}

	got, err := h.UpdateConfigFromConfigMap(map[string]string{"CPU": "0.05", "RAM": "0.006"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.CPU != "0.05" || got.RAM != "0.006" || cfg.customPricing.CPU != "0.05" {
		t.Fatalf("expected the ConfigMap rates to be stored, got %+v", got)
	}

	cfg.err = fmt.Errorf("write failed")
	if _, err := h.UpdateConfigFromConfigMap(map[string]string{"CPU": "0.06"}); err == nil {
		t.Fatalf("expected the config error to propagate")
	}
}

func TestHuawei_UpdateConfig(t *testing.T) {
	cfg := &updatableProviderConfig{fakeProviderConfig: fakeProviderConfig{customPricing: &models.CustomPricing{}}}
	h := &Huawei{Config: cfg}

	got, err := h.UpdateConfig(strings.NewReader(`{"CPU": "0.05", "storage": "0.0002"}`), "customPricing")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.CPU != "0.05" || got.Storage != "0.0002" {
		t.Fatalf("expected the posted rates to be stored, got %+v", got)
	}

	if _, err := h.UpdateConfig(strings.NewReader(`{"CPU": 0.05}`), "customPricing"); err == nil {
		t.Fatalf("expected an error for a non-string value")
	}
	if _, err := h.UpdateConfig(strings.NewReader(`not json`), "customPricing"); err == nil {
		t.Fatalf("expected an error for an invalid body")
	}
}

func TestHuawei_PricingSourceSummary(t *testing.T) {
	h := &Huawei{Pricing: map[string]*HuaweiPricing{
		"la-south-2,c7n.2xlarge.4,linux": {NodeAttributes: &HuaweiNodeAttributes{Price: "1.234"}},
	}}
	summary, ok := h.PricingSourceSummary().(map[string]*HuaweiPricing)
	if !ok {
		t.Fatalf("expected map[string]*HuaweiPricing summary")
	}
	if summary["la-south-2,c7n.2xlarge.4,linux"].NodeAttributes.Price != "1.234" {
		t.Fatalf("unexpected summary: %+v", summary)
	}
}

// TestHuawei_DownloadPricingData_LiveBSS drives DownloadPricingData end-to-end
// against an httptest server standing in for the Huawei Cloud BSS demandPrice
// endpoint, using a real cluster node/PV fixture (labels captured from a live CCE
// cluster) so both the ECS and EVS pricing paths are exercised.
func TestHuawei_DownloadPricingData_LiveBSS(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v2/bills/ratings/on-demand-resources" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("reading request body: %v", err)
		}
		// The EVS product must be queried with the Huawei volume type code (SSD, from
		// the StorageClass's everest.io/disk-volume-type parameter), never the raw
		// Kubernetes StorageClass name -- querying with the k8s name is exactly the
		// live bug this test guards against (BSS returns "Product not found").
		if strings.Contains(string(body), "csi-disk-dss") {
			t.Fatalf("request body must not contain the raw StorageClass name, got: %s", body)
		}
		if !strings.Contains(string(body), `"resource_spec":"SSD"`) {
			t.Fatalf("expected EVS resource_spec \"SSD\" in request body, got: %s", body)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{
			"product_rating_results": [
				{"id": "node-0", "amount": "1.234"},
				{"id": "pv-1", "amount": "0.700"},
				{"id": "lb-2", "amount": "0.053"}
			]
		}`))
	}))
	defer server.Close()

	bssEndpointOverride = server.URL
	defer func() { bssEndpointOverride = "" }()

	t.Setenv(env.HuaweiAccessKeyIDEnvVar, "test-ak")
	t.Setenv(env.HuaweiAccessKeySecretEnvVar, "test-sk")
	t.Setenv(env.HuaweiDomainIDEnvVar, "test-domain")

	node := &clustercache.Node{
		Labels: map[string]string{
			"topology.kubernetes.io/region":    "la-south-2",
			"node.kubernetes.io/instance-type": "c7n.2xlarge.4",
			"kubernetes.io/os":                 "linux",
		},
	}
	node.Status.Capacity = v1.ResourceList{
		v1.ResourceCPU:    *resource.NewQuantity(8, resource.DecimalSI),
		v1.ResourceMemory: *resource.NewQuantity(16*1024*1024*1024, resource.BinarySI),
	}
	pv := &clustercache.PersistentVolume{Spec: v1.PersistentVolumeSpec{StorageClassName: "csi-disk-dss"}}
	storageClass := &clustercache.StorageClass{
		Name:       "csi-disk-dss",
		Parameters: map[string]string{everestDiskVolumeTypeParam: "SSD"},
	}

	h := &Huawei{
		Clientset: &clustercache.MockClusterCache{
			Nodes:             []*clustercache.Node{node},
			PersistentVolumes: []*clustercache.PersistentVolume{pv},
			StorageClasses:    []*clustercache.StorageClass{storageClass},
		},
		Config:        &fakeProviderConfig{customPricing: &models.CustomPricing{CPU: "0.0345", RAM: "0.0046", ProjectID: "test-project"}},
		ClusterRegion: "la-south-2",
	}

	if err := h.DownloadPricingData(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	nodePricing, ok := h.Pricing["la-south-2,c7n.2xlarge.4,linux"]
	if !ok || nodePricing.NodeAttributes == nil {
		t.Fatalf("expected live node pricing to be populated, got %+v", h.Pricing)
	}
	if nodePricing.NodeAttributes.Price != "1.234" {
		t.Fatalf("expected node price 1.234, got %s", nodePricing.NodeAttributes.Price)
	}

	pvPricing, ok := h.Pricing["la-south-2,csi-disk-dss"]
	if !ok || pvPricing.PVAttributes == nil {
		t.Fatalf("expected live PV pricing to be populated, got %+v", h.Pricing)
	}
	expectedPVPrice := "0.007" // 0.700 / evsReferenceSizeGB(100)
	if pvPricing.PVAttributes.Price != expectedPVPrice {
		t.Fatalf("expected PV price %s, got %s", expectedPVPrice, pvPricing.PVAttributes.Price)
	}

	lbPricing, ok := h.Pricing["la-south-2,LoadBalancerUsage"]
	if !ok || lbPricing.LoadBalancerAttributes == nil {
		t.Fatalf("expected live load balancer pricing to be populated, got %+v", h.Pricing)
	}
	if lbPricing.LoadBalancerAttributes.Price != "0.053" {
		t.Fatalf("expected load balancer price 0.053, got %s", lbPricing.LoadBalancerAttributes.Price)
	}
	if lb, err := h.LoadBalancerPricing(); err != nil || lb.Cost != 0.053 {
		t.Fatalf("expected LoadBalancerPricing to return live cost 0.053, got %v/%v", lb, err)
	}
}

// TestHuawei_DownloadPricingData_MissingCredentials verifies that a missing
// AK/SK/domain ID degrades gracefully to the static fallback instead of failing
// DownloadPricingData outright.
// withoutHuaweiCredentials makes a test see no credentials at all, whatever
// the machine running it has: no AK/SK, domain or project in the environment,
// an IAM agency lookup that fails, empty agency and project-ID caches, and a
// metadata service that answers 404. BSS fails the test if it is reached.
func withoutHuaweiCredentials(t *testing.T) {
	t.Helper()
	t.Setenv(env.HuaweiAccessKeyIDEnvVar, "")
	t.Setenv(env.HuaweiAccessKeySecretEnvVar, "")
	t.Setenv(env.HuaweiDomainIDEnvVar, "")
	t.Setenv(env.HuaweiProjectIDEnvVar, "")

	origAgency := agencyCredentials
	agencyCredentials = func() (auth.ICredential, error) {
		return nil, fmt.Errorf("no IAM agency in tests")
	}

	metadata := httptest.NewServer(http.NotFoundHandler())
	origMetadata := metadataEndpoint
	metadataEndpoint = metadata.URL

	bss := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("BSS must not be reached without credentials, got %s %s", r.Method, r.URL.Path)
	}))
	origBSS := bssEndpointOverride
	bssEndpointOverride = bss.URL

	resetCaches := func() {
		agencyCredsCacheMu.Lock()
		agencyCredsCacheValue = nil
		agencyCredsCacheMu.Unlock()
		projectIDCacheMu.Lock()
		projectIDCacheValue = ""
		projectIDCacheMu.Unlock()
	}
	resetCaches()

	t.Cleanup(func() {
		agencyCredentials = origAgency
		metadataEndpoint = origMetadata
		bssEndpointOverride = origBSS
		metadata.Close()
		bss.Close()
		resetCaches()
	})
}

func TestHuawei_DownloadPricingData_MissingCredentials(t *testing.T) {
	withoutHuaweiCredentials(t)
	node := &clustercache.Node{
		Labels: map[string]string{
			"topology.kubernetes.io/region":    "la-south-2",
			"node.kubernetes.io/instance-type": "c7n.2xlarge.4",
			"kubernetes.io/os":                 "linux",
		},
	}
	h := &Huawei{
		Clientset: &clustercache.MockClusterCache{Nodes: []*clustercache.Node{node}},
		Config:    &fakeProviderConfig{customPricing: &models.CustomPricing{CPU: "0.0345", RAM: "0.0046"}},
	}
	if err := h.DownloadPricingData(); err != nil {
		t.Fatalf("expected graceful fallback, got error: %v", err)
	}
	if len(h.Pricing) != 0 {
		t.Fatalf("expected no live pricing without credentials, got %+v", h.Pricing)
	}
	if h.BaseCPUPrice != "0.0345" {
		t.Fatalf("expected static fallback to still load, got %s", h.BaseCPUPrice)
	}
}

// TestHuawei_DownloadPricingData_SkipsUnmappedStorageClass verifies that a PV whose
// StorageClass has no everest.io/disk-volume-type parameter (or no matching
// StorageClass at all) is skipped for live EVS pricing rather than querying BSS with
// the raw Kubernetes StorageClass name, which the live API rejects with "Product not
// found" (see TestHuawei_DownloadPricingData_LiveBSS).
func TestHuawei_DownloadPricingData_SkipsUnmappedStorageClass(t *testing.T) {
	var requestBody string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("reading request body: %v", err)
		}
		requestBody = string(body)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"product_rating_results": []}`))
	}))
	defer server.Close()

	bssEndpointOverride = server.URL
	defer func() { bssEndpointOverride = "" }()

	t.Setenv(env.HuaweiAccessKeyIDEnvVar, "test-ak")
	t.Setenv(env.HuaweiAccessKeySecretEnvVar, "test-sk")
	t.Setenv(env.HuaweiDomainIDEnvVar, "test-domain")

	pv := &clustercache.PersistentVolume{Spec: v1.PersistentVolumeSpec{StorageClassName: "unmapped-class"}}
	h := &Huawei{
		Clientset:     &clustercache.MockClusterCache{PersistentVolumes: []*clustercache.PersistentVolume{pv}},
		Config:        &fakeProviderConfig{customPricing: &models.CustomPricing{CPU: "0.0345", RAM: "0.0046", ProjectID: "test-project"}},
		ClusterRegion: "la-south-2",
	}

	if err := h.DownloadPricingData(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// A BSS request is still expected here for the cluster region's load balancer
	// price (queried per-region, independent of PVs/nodes), but it must never
	// reference the unmapped StorageClass name.
	if requestBody == "" {
		t.Fatalf("expected a BSS request for the cluster region's load balancer price")
	}
	if strings.Contains(requestBody, "unmapped-class") {
		t.Fatalf("must not query BSS with the raw, unmapped StorageClass name, got: %s", requestBody)
	}
	if len(h.Pricing) != 0 {
		t.Fatalf("expected no live pricing for an unmapped storage class or empty ratings response, got %+v", h.Pricing)
	}
}

// TestHuawei_DownloadPricingData_KeepsPricesWhenBSSFails covers a refresh that
// yields no prices -- an error, or a response with no usable rating results: the
// prices from the last successful download must survive it rather than every
// node, disk and load balancer dropping to the static rates.
func TestHuawei_DownloadPricingData_KeepsPricesWhenBSSFails(t *testing.T) {
	cases := map[string]http.HandlerFunc{
		"error": func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, `{"error_code": "CBC.0999", "error_msg": "unavailable"}`, http.StatusServiceUnavailable)
		},
		"empty results": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"product_rating_results": []}`))
		},
		"no results": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{}`))
		},
		"no usable results": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"product_rating_results": [{"id": "node-0"}, {"id": "unknown", "amount": "1.0"}]}`))
		},
	}

	for name, handler := range cases {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(handler)
			defer server.Close()

			bssEndpointOverride = server.URL
			defer func() { bssEndpointOverride = "" }()

			t.Setenv(env.HuaweiAccessKeyIDEnvVar, "test-ak")
			t.Setenv(env.HuaweiAccessKeySecretEnvVar, "test-sk")
			t.Setenv(env.HuaweiDomainIDEnvVar, "test-domain")

			node := &clustercache.Node{
				Labels: map[string]string{
					"topology.kubernetes.io/region":    "la-south-2",
					"node.kubernetes.io/instance-type": "c7n.2xlarge.4",
					"kubernetes.io/os":                 "linux",
				},
			}
			previous := map[string]*HuaweiPricing{
				"la-south-2,c7n.2xlarge.4,linux": {NodeAttributes: &HuaweiNodeAttributes{Type: "c7n.2xlarge.4", OS: "linux", Price: "1.234"}},
			}
			h := &Huawei{
				Clientset:     &clustercache.MockClusterCache{Nodes: []*clustercache.Node{node}},
				Config:        &fakeProviderConfig{customPricing: &models.CustomPricing{CPU: "0.0345", RAM: "0.0046", ProjectID: "test-project"}},
				ClusterRegion: "la-south-2",
				Pricing:       previous,
			}

			if err := h.DownloadPricingData(); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			got, ok := h.Pricing["la-south-2,c7n.2xlarge.4,linux"]
			if !ok || got.NodeAttributes == nil || got.NodeAttributes.Price != "1.234" {
				t.Fatalf("expected the previous live price to be kept, got %+v", h.Pricing)
			}
		})
	}
}

// TestHuawei_DownloadPricingData_KeepsPricesMissingFromPartialResponse covers a
// response that prices only some of the products asked for: the others keep
// their previous live price instead of dropping to the static rates.
func TestHuawei_DownloadPricingData_KeepsPricesMissingFromPartialResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// node-0 is priced, the load balancer (lb-1) is not.
		w.Write([]byte(`{"product_rating_results": [{"id": "node-0", "amount": "2.000"}]}`))
	}))
	defer server.Close()

	bssEndpointOverride = server.URL
	defer func() { bssEndpointOverride = "" }()

	t.Setenv(env.HuaweiAccessKeyIDEnvVar, "test-ak")
	t.Setenv(env.HuaweiAccessKeySecretEnvVar, "test-sk")
	t.Setenv(env.HuaweiDomainIDEnvVar, "test-domain")

	node := &clustercache.Node{
		Labels: map[string]string{
			"topology.kubernetes.io/region":    "la-south-2",
			"node.kubernetes.io/instance-type": "c7n.2xlarge.4",
			"kubernetes.io/os":                 "linux",
		},
	}
	h := &Huawei{
		Clientset:     &clustercache.MockClusterCache{Nodes: []*clustercache.Node{node}},
		Config:        &fakeProviderConfig{customPricing: &models.CustomPricing{CPU: "0.0345", RAM: "0.0046", ProjectID: "test-project"}},
		ClusterRegion: "la-south-2",
		Pricing: map[string]*HuaweiPricing{
			"la-south-2,c7n.2xlarge.4,linux": {NodeAttributes: &HuaweiNodeAttributes{Type: "c7n.2xlarge.4", OS: "linux", Price: "1.234"}},
			"la-south-2,LoadBalancerUsage":   {LoadBalancerAttributes: &HuaweiLoadBalancerAttributes{Price: "0.071"}},
		},
	}

	if err := h.DownloadPricingData(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := h.Pricing["la-south-2,c7n.2xlarge.4,linux"]; got == nil || got.NodeAttributes.Price != "2" {
		t.Fatalf("expected the node to take the new price, got %+v", got)
	}
	if got := h.Pricing["la-south-2,LoadBalancerUsage"]; got == nil || got.LoadBalancerAttributes.Price != "0.071" {
		t.Fatalf("expected the load balancer to keep its previous price, got %+v", got)
	}
}

// ratingServer stands in for BSS demandPrice: it rejects, with status, any
// request containing a product whose resource_spec is in rejected, and
// otherwise prices every product it is sent at 1.5. It counts requests.
func ratingServer(t *testing.T, status int, rejected ...string) *int {
	t.Helper()
	var requests int
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests++
		mu.Unlock()
		var body struct {
			ProductInfos []struct {
				ID           string `json:"id"`
				ResourceSpec string `json:"resource_spec"`
			} `json:"product_infos"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decoding demandPrice request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		for _, p := range body.ProductInfos {
			if slices.Contains(rejected, p.ResourceSpec) {
				w.WriteHeader(status)
				w.Write([]byte(`{"error_code": "CBC.30000052", "error_msg": "Product not found."}`))
				return
			}
		}
		results := make([]string, 0, len(body.ProductInfos))
		for _, p := range body.ProductInfos {
			results = append(results, `{"id": "`+p.ID+`", "amount": "1.5"}`)
		}
		w.Write([]byte(`{"product_rating_results": [` + strings.Join(results, ",") + `]}`))
	}))
	t.Cleanup(server.Close)

	bssEndpointOverride = server.URL
	t.Cleanup(func() { bssEndpointOverride = "" })
	t.Setenv(env.HuaweiAccessKeyIDEnvVar, "test-ak")
	t.Setenv(env.HuaweiAccessKeySecretEnvVar, "test-sk")
	t.Setenv(env.HuaweiDomainIDEnvVar, "test-domain")
	return &requests
}

func flavorNode(instanceType string) *clustercache.Node {
	return &clustercache.Node{Labels: map[string]string{
		"topology.kubernetes.io/region":    "la-south-2",
		"node.kubernetes.io/instance-type": instanceType,
		"kubernetes.io/os":                 "linux",
	}}
}

// TestHuawei_DownloadPricingData_IsolatesRejectedProducts covers a batch BSS
// rejects because of one product it cannot price: the other flavors, the
// disk and the load balancer must still get live prices.
func TestHuawei_DownloadPricingData_IsolatesRejectedProducts(t *testing.T) {
	requests := ratingServer(t, http.StatusBadRequest, "x9.unsupported.linux")

	pv := &clustercache.PersistentVolume{Spec: v1.PersistentVolumeSpec{StorageClassName: "csi-disk-dss"}}
	h := &Huawei{
		Clientset: &clustercache.MockClusterCache{
			Nodes:             []*clustercache.Node{flavorNode("c7n.2xlarge.4"), flavorNode("x9.unsupported"), flavorNode("s7n.large.2")},
			PersistentVolumes: []*clustercache.PersistentVolume{pv},
			StorageClasses: []*clustercache.StorageClass{{
				Name:       "csi-disk-dss",
				Parameters: map[string]string{everestDiskVolumeTypeParam: "SSD"},
			}},
		},
		Config:        &fakeProviderConfig{customPricing: &models.CustomPricing{CPU: "0.0345", RAM: "0.0046", ProjectID: "test-project"}},
		ClusterRegion: "la-south-2",
	}

	if err := h.DownloadPricingData(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, features := range []string{"la-south-2,c7n.2xlarge.4,linux", "la-south-2,s7n.large.2,linux", "la-south-2,csi-disk-dss", "la-south-2,LoadBalancerUsage"} {
		if _, ok := h.Pricing[features]; !ok {
			t.Errorf("expected a live price for %s despite the rejected product, got %+v", features, h.Pricing)
		}
	}
	if _, ok := h.Pricing["la-south-2,x9.unsupported,linux"]; ok {
		t.Errorf("expected the rejected flavor to stay unpriced")
	}
	if *requests > maxRatingRequests {
		t.Errorf("expected at most %d requests, got %d", maxRatingRequests, *requests)
	}
}

// TestHuawei_DownloadPricingData_DoesNotSplitOnAuthFailure checks that a
// failure smaller batches cannot fix -- here permissions -- is not retried
// product by product: one request, and the previous prices stay.
func TestHuawei_DownloadPricingData_DoesNotSplitOnAuthFailure(t *testing.T) {
	requests := ratingServer(t, http.StatusForbidden, "c7n.2xlarge.4.linux")

	previous := map[string]*HuaweiPricing{
		"la-south-2,c7n.2xlarge.4,linux": {NodeAttributes: &HuaweiNodeAttributes{Price: "1.234"}},
	}
	h := &Huawei{
		Clientset:     &clustercache.MockClusterCache{Nodes: []*clustercache.Node{flavorNode("c7n.2xlarge.4"), flavorNode("s7n.large.2")}},
		Config:        &fakeProviderConfig{customPricing: &models.CustomPricing{CPU: "0.0345", RAM: "0.0046", ProjectID: "test-project"}},
		ClusterRegion: "la-south-2",
		Pricing:       previous,
	}

	if err := h.DownloadPricingData(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if *requests != 1 {
		t.Errorf("expected a single request for a permission failure, got %d", *requests)
	}
	if got := h.Pricing["la-south-2,c7n.2xlarge.4,linux"]; got == nil || got.NodeAttributes.Price != "1.234" {
		t.Errorf("expected the previous price to be kept, got %+v", h.Pricing)
	}
}

// TestHuawei_GetConfigDoesNotWriteSharedConfig checks the defaults GetConfig
// fills in go on a copy: the config's cached value is shared with other
// goroutines and only Update may change it, under the config's lock.
func TestHuawei_GetConfigDoesNotWriteSharedConfig(t *testing.T) {
	shared := &models.CustomPricing{CPU: "0.0345"}
	h := &Huawei{Config: &fakeProviderConfig{customPricing: shared}}

	c, err := h.GetConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.CurrencyCode != "USD" || c.CPU != "0.0345" {
		t.Fatalf("expected the defaults on top of the config, got %+v", c)
	}
	if shared.CurrencyCode != "" || shared.Discount != "" || shared.DefaultLBPrice != "" {
		t.Fatalf("GetConfig must not write the shared config, got %+v", shared)
	}
}

// TestHuawei_PVPricing_RefreshesOnCacheMiss covers a storage class that got
// its first PV after the last download: the lookup that misses answers with
// the static default without waiting, and starts a background refresh after
// which the class has its live price.
func TestHuawei_PVPricing_RefreshesOnCacheMiss(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"product_rating_results": [{"id": "pv-0", "amount": "0.700"}]}`))
	}))
	defer server.Close()

	bssEndpointOverride = server.URL
	defer func() { bssEndpointOverride = "" }()

	t.Setenv(env.HuaweiAccessKeyIDEnvVar, "test-ak")
	t.Setenv(env.HuaweiAccessKeySecretEnvVar, "test-sk")
	t.Setenv(env.HuaweiDomainIDEnvVar, "test-domain")

	pv := &clustercache.PersistentVolume{Spec: v1.PersistentVolumeSpec{StorageClassName: "csi-disk-dss"}}
	h := &Huawei{
		Clientset: &clustercache.MockClusterCache{
			PersistentVolumes: []*clustercache.PersistentVolume{pv},
			StorageClasses: []*clustercache.StorageClass{{
				Name:       "csi-disk-dss",
				Parameters: map[string]string{everestDiskVolumeTypeParam: "SSD"},
			}},
		},
		Config:        &fakeProviderConfig{customPricing: &models.CustomPricing{Storage: "0.0001512", ProjectID: "test-project"}},
		ClusterRegion: "la-south-2",
		Pricing:       map[string]*HuaweiPricing{},
	}

	key := h.GetPVKey(pv, map[string]string{}, "la-south-2")
	got, err := h.PVPricing(key)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Cost != "0.0001512" {
		t.Fatalf("expected the static default while the refresh runs, got %q", got.Cost)
	}

	h.refreshes.Wait()
	got, err = h.PVPricing(key)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Cost != "0.007" { // 0.700 / evsReferenceSizeGB(100)
		t.Fatalf("expected live PV price 0.007 after refresh, got %q", got.Cost)
	}
}

// TestHuawei_PricingLookupsDoNotWaitOnBSS holds BSS's answer and checks that
// pricing lookups keep answering meanwhile: both a cache miss, which starts a
// refresh, and a lookup made while a full download is in flight.
func TestHuawei_PricingLookupsDoNotWaitOnBSS(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"product_rating_results": [{"id": "node-0", "amount": "2.000"}]}`))
	}))
	defer server.Close()
	var released sync.Once
	releaseBSS := func() { released.Do(func() { close(release) }) }
	defer releaseBSS()

	bssEndpointOverride = server.URL
	defer func() { bssEndpointOverride = "" }()
	t.Setenv(env.HuaweiAccessKeyIDEnvVar, "test-ak")
	t.Setenv(env.HuaweiAccessKeySecretEnvVar, "test-sk")
	t.Setenv(env.HuaweiDomainIDEnvVar, "test-domain")

	node := flavorNode("c7n.2xlarge.4")
	h := &Huawei{
		Clientset:    &clustercache.MockClusterCache{Nodes: []*clustercache.Node{node}},
		Config:       &fakeProviderConfig{customPricing: &models.CustomPricing{CPU: "0.0345", RAM: "0.0046", ProjectID: "test-project"}},
		BaseCPUPrice: "0.0345",
		BaseRAMPrice: "0.0046",
		Pricing: map[string]*HuaweiPricing{
			"la-south-2,s7n.large.2,linux": {NodeAttributes: &HuaweiNodeAttributes{Price: "0.5"}},
		},
	}

	answers := make(chan *models.Node, 2)
	go func() {
		// A miss: starts a background refresh, which blocks on BSS.
		n, _, _ := h.NodePricing(h.GetKey(node.Labels, node))
		answers <- n
	}()
	go func() {
		// A cached flavor, looked up while that refresh is in flight.
		other := flavorNode("s7n.large.2")
		n, _, _ := h.NodePricing(h.GetKey(other.Labels, other))
		answers <- n
	}()
	for i := 0; i < 2; i++ {
		select {
		case n := <-answers:
			if n == nil || (n.Cost != "0.5" && !n.UsesBaseCPUPrice) {
				t.Errorf("expected a cached or base price, got %+v", n)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("a pricing lookup waited on BSS")
		}
	}

	// A full download in flight does not hold the lock lookups need either.
	downloaded := make(chan error, 1)
	go func() { downloaded <- h.DownloadPricingData() }()
	looked := make(chan struct{})
	go func() {
		h.LoadBalancerPricing()
		h.PricingSourceStatus()
		close(looked)
	}()
	select {
	case <-looked:
	case <-time.After(5 * time.Second):
		t.Fatalf("a pricing lookup waited on an in-flight download")
	}

	// Let BSS answer and wait for the refresh and the download to land.
	releaseBSS()
	h.refreshes.Wait()
	if err := <-downloaded; err != nil {
		t.Fatalf("unexpected download error: %v", err)
	}
	if got := h.Pricing["la-south-2,c7n.2xlarge.4,linux"]; got == nil || got.NodeAttributes.Price != "2" {
		t.Fatalf("expected the refreshed live price once BSS answered, got %+v", got)
	}
}

// TestHuawei_GetConfigDefaults covers the defaults GetConfig fills in. The
// currency default is USD, matching both the international BSS endpoint this
// provider queries and the rates in configs/huawei.json.
func TestHuawei_GetConfigDefaults(t *testing.T) {
	h := &Huawei{Config: &fakeProviderConfig{customPricing: &models.CustomPricing{}}}

	c, err := h.GetConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.CurrencyCode != "USD" {
		t.Errorf("CurrencyCode = %q, want USD", c.CurrencyCode)
	}
	if c.DefaultLBPrice != "0.053" {
		t.Errorf("DefaultLBPrice = %q, want 0.053", c.DefaultLBPrice)
	}
	if c.Discount != "0%" {
		t.Errorf("Discount = %q, want 0%%", c.Discount)
	}
	if c.NegotiatedDiscount != "0%" {
		t.Errorf("NegotiatedDiscount = %q, want 0%%", c.NegotiatedDiscount)
	}
}

// TestHuawei_GetConfigKeepsConfiguredValues checks the defaults do not override
// values present in the config.
func TestHuawei_GetConfigKeepsConfiguredValues(t *testing.T) {
	h := &Huawei{Config: &fakeProviderConfig{customPricing: &models.CustomPricing{
		CurrencyCode:   "CNY",
		DefaultLBPrice: "0.071",
	}}}

	c, err := h.GetConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.CurrencyCode != "CNY" {
		t.Errorf("CurrencyCode = %q, want CNY", c.CurrencyCode)
	}
	if c.DefaultLBPrice != "0.071" {
		t.Errorf("DefaultLBPrice = %q, want 0.071", c.DefaultLBPrice)
	}
}

// swappableProviderConfig is a ProviderConfig whose pricing can be replaced
// while other goroutines read it, as a settings update does.
type swappableProviderConfig struct {
	fakeProviderConfig
	mu sync.Mutex
}

func (s *swappableProviderConfig) GetCustomPricingData() (*models.CustomPricing, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fakeProviderConfig.GetCustomPricingData()
}

func (s *swappableProviderConfig) set(c *models.CustomPricing) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.customPricing = c
}

// projectBSS stands in for BSS demandPrice. It prices node-0 by the
// request's project (proj-a at 2, proj-b at 3), reports each request's project
// on arrived, and holds each project's requests until release(project), so a
// test can tell which project's download published what.
type projectBSS struct {
	arrived chan string
	mu      sync.Mutex
	gates   map[string]chan struct{}
}

func newProjectBSS(t *testing.T) *projectBSS {
	t.Helper()
	b := &projectBSS{arrived: make(chan string, 8), gates: map[string]chan struct{}{}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			ProjectID string `json:"project_id"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		b.arrived <- body.ProjectID
		<-b.gate(body.ProjectID)
		amount := map[string]string{"proj-a": "2", "proj-b": "3"}[body.ProjectID]
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"product_rating_results": [{"id": "node-0", "amount": "` + amount + `"}]}`))
	}))
	// Cleanups run last-registered first: every gate opens before the server
	// closes, which waits for the requests in flight.
	t.Cleanup(server.Close)
	t.Cleanup(b.releaseAll)

	bssEndpointOverride = server.URL
	t.Cleanup(func() { bssEndpointOverride = "" })
	t.Setenv(env.HuaweiAccessKeyIDEnvVar, "test-ak")
	t.Setenv(env.HuaweiAccessKeySecretEnvVar, "test-sk")
	t.Setenv(env.HuaweiDomainIDEnvVar, "test-domain")
	t.Setenv(env.HuaweiProjectIDEnvVar, "")
	return b
}

func (b *projectBSS) gate(project string) chan struct{} {
	b.mu.Lock()
	defer b.mu.Unlock()
	g, ok := b.gates[project]
	if !ok {
		g = make(chan struct{})
		b.gates[project] = g
	}
	return g
}

// release lets the requests for project through, now and from then on.
func (b *projectBSS) release(project string) {
	g := b.gate(project)
	b.mu.Lock()
	defer b.mu.Unlock()
	select {
	case <-g:
	default:
		close(g)
	}
}

func (b *projectBSS) releaseAll() {
	for _, project := range []string{"proj-a", "proj-b", ""} {
		b.release(project)
	}
}

// nodePrice returns the live price of the flavorNode("c7n.2xlarge.4") node.
func nodePrice(h *Huawei) string {
	h.DownloadPricingDataLock.RLock()
	defer h.DownloadPricingDataLock.RUnlock()
	if p := h.Pricing["la-south-2,c7n.2xlarge.4,linux"]; p != nil && p.NodeAttributes != nil {
		return p.NodeAttributes.Price
	}
	return ""
}

// TestHuawei_DownloadPricingData_KeepsConfigUpdatedMeanwhile applies a config
// update while a download waits on BSS. The update and pricing lookups must
// not wait for the download, and the download, once it lands, must not put
// back the rates and project ID it started from: with the same project its
// live prices are published under the new rates; with a new project they
// were fetched for the old one and are dropped, and a download for the new
// project replaces them.
func TestHuawei_DownloadPricingData_KeepsConfigUpdatedMeanwhile(t *testing.T) {
	cases := map[string]struct {
		updatedProject string
		wantPrice      string // once the in-flight download lands
		wantFinal      string // once every download has landed
	}{
		"rates change":   {updatedProject: "proj-a", wantPrice: "2", wantFinal: "2"},
		"project change": {updatedProject: "proj-b", wantPrice: "1.0", wantFinal: "3"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			bss := newProjectBSS(t)

			node := flavorNode("c7n.2xlarge.4")
			cfg := &swappableProviderConfig{fakeProviderConfig: fakeProviderConfig{customPricing: &models.CustomPricing{CPU: "0.01", RAM: "0.001", ProjectID: "proj-a"}}}
			h := &Huawei{
				Clientset: &clustercache.MockClusterCache{Nodes: []*clustercache.Node{node}},
				Config:    cfg,
				ProjectID: "proj-a",
				Pricing: map[string]*HuaweiPricing{
					"la-south-2,c7n.2xlarge.4,linux": {NodeAttributes: &HuaweiNodeAttributes{Price: "1.0"}},
				},
			}
			defer h.refreshes.Wait()

			downloaded := make(chan error, 1)
			go func() { downloaded <- h.DownloadPricingData() }()
			<-bss.arrived

			// The update lands while BSS has not answered yet.
			cfg.set(&models.CustomPricing{CPU: "0.02", RAM: "0.002", ProjectID: tc.updatedProject})
			done := make(chan error, 1)
			go func() {
				err := h.RefreshCustomPricing()
				h.NodePricing(h.GetKey(node.Labels, node))
				done <- err
			}()
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("the config update or a pricing lookup waited on BSS")
			}

			// Only proj-a's download goes through: one for proj-b, if any,
			// waits at BSS and cannot publish yet.
			bss.release("proj-a")
			if err := <-downloaded; err != nil {
				t.Fatalf("unexpected download error: %v", err)
			}
			h.DownloadPricingDataLock.RLock()
			if h.BaseCPUPrice != "0.02" || h.BaseRAMPrice != "0.002" {
				t.Errorf("the download put back the old rates: cpu %q ram %q", h.BaseCPUPrice, h.BaseRAMPrice)
			}
			if h.ProjectID != tc.updatedProject {
				t.Errorf("the download put back project %q, want %q", h.ProjectID, tc.updatedProject)
			}
			h.DownloadPricingDataLock.RUnlock()
			if got := nodePrice(h); got != tc.wantPrice {
				t.Errorf("expected the node priced at %s, got %q", tc.wantPrice, got)
			}

			bss.release("proj-b")
			h.refreshes.Wait()
			if got := nodePrice(h); got != tc.wantFinal {
				t.Errorf("expected the node priced at %s once every download landed, got %q", tc.wantFinal, got)
			}
		})
	}
}

// lockedProviderConfig is a swappableProviderConfig that applies updates under its lock
// the way ProviderConfig does, so tests can go through UpdateConfig and
// UpdateConfigFromConfigMap.
type lockedProviderConfig struct {
	swappableProviderConfig
}

func (u *lockedProviderConfig) Update(f func(*models.CustomPricing) error) (*models.CustomPricing, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	c := *u.customPricing
	if err := f(&c); err != nil {
		return nil, err
	}
	u.customPricing = &c
	return &c, nil
}

func (u *lockedProviderConfig) UpdateFromMap(a map[string]string) (*models.CustomPricing, error) {
	return u.Update(func(c *models.CustomPricing) error {
		for k, v := range a {
			if err := models.SetCustomPricingField(c, utils.ToTitle.String(k), v); err != nil {
				return err
			}
		}
		return nil
	})
}

// TestHuawei_UpdateConfig_ReloadsProjectID checks that a config update which
// only changes the project ID reaches the provider: the settings pipeline does
// not call RefreshCustomPricing for it, since it only compares rates.
func TestHuawei_UpdateConfig_ReloadsProjectID(t *testing.T) {
	t.Setenv(env.HuaweiProjectIDEnvVar, "")
	cfg := &lockedProviderConfig{swappableProviderConfig{fakeProviderConfig: fakeProviderConfig{customPricing: &models.CustomPricing{CPU: "0.01", ProjectID: "proj-a"}}}}
	h := &Huawei{Config: cfg, ProjectID: "proj-a"}
	// Each project change starts a download (with no Clientset, it queries nothing).
	defer h.refreshes.Wait()

	if _, err := h.UpdateConfigFromConfigMap(map[string]string{"projectID": "proj-b"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h.ProjectID != "proj-b" || h.configGeneration != 1 {
		t.Fatalf("ConfigMap update not reloaded: project %q, generation %d", h.ProjectID, h.configGeneration)
	}

	if _, err := h.UpdateConfig(strings.NewReader(`{"projectID": "proj-c"}`), ""); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h.ProjectID != "proj-c" || h.configGeneration != 2 {
		t.Fatalf("JSON update not reloaded: project %q, generation %d", h.ProjectID, h.configGeneration)
	}

	// A rejected update changes nothing.
	if _, err := h.UpdateConfig(strings.NewReader(`{"projectID": 1}`), ""); err == nil {
		t.Fatalf("expected an error for a non-string value")
	}
	if h.ProjectID != "proj-c" || h.configGeneration != 2 {
		t.Fatalf("rejected update was applied: project %q, generation %d", h.ProjectID, h.configGeneration)
	}
}

// TestHuawei_DownloadPricingData_DropsResultsAfterConfigMapProjectChange
// changes only the project ID through UpdateConfigFromConfigMap while a
// download waits on BSS. The download must drop the prices it fetched for the
// previous project and must not put that project ID back; the new project's
// prices then come from a download of their own.
func TestHuawei_DownloadPricingData_DropsResultsAfterConfigMapProjectChange(t *testing.T) {
	bss := newProjectBSS(t)

	node := flavorNode("c7n.2xlarge.4")
	cfg := &lockedProviderConfig{swappableProviderConfig{fakeProviderConfig: fakeProviderConfig{customPricing: &models.CustomPricing{CPU: "0.01", RAM: "0.001", ProjectID: "proj-a"}}}}
	h := &Huawei{
		Clientset: &clustercache.MockClusterCache{Nodes: []*clustercache.Node{node}},
		Config:    cfg,
		ProjectID: "proj-a",
		Pricing: map[string]*HuaweiPricing{
			"la-south-2,c7n.2xlarge.4,linux": {NodeAttributes: &HuaweiNodeAttributes{Price: "1.0"}},
		},
	}
	defer h.refreshes.Wait()

	downloaded := make(chan error, 1)
	go func() { downloaded <- h.DownloadPricingData() }()
	<-bss.arrived

	updated := make(chan error, 1)
	go func() {
		_, err := h.UpdateConfigFromConfigMap(map[string]string{"projectID": "proj-b"})
		updated <- err
	}()
	select {
	case err := <-updated:
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("the ConfigMap update waited on BSS")
	}

	bss.release("proj-a")
	if err := <-downloaded; err != nil {
		t.Fatalf("unexpected download error: %v", err)
	}
	h.DownloadPricingDataLock.RLock()
	project := h.ProjectID
	h.DownloadPricingDataLock.RUnlock()
	if project != "proj-b" {
		t.Errorf("the download put back project %q, want proj-b", project)
	}
	if got := nodePrice(h); got != "1.0" {
		t.Errorf("expected proj-a prices dropped and the node kept at 1.0, got %q", got)
	}

	bss.release("proj-b")
	h.refreshes.Wait()
	if got := nodePrice(h); got != "3" {
		t.Errorf("expected proj-b's price 3 once its download landed, got %q", got)
	}
}

// TestHuawei_ProjectChangeRefreshesCachedPricing covers a fully cached
// cluster: no lookup misses the cache, so nothing else would refresh the
// prices, and a cache-miss refresh ran just before, so the cooldown would
// discard one. A ConfigMap update of the project ID alone must still replace
// the previous project's prices with the new one's.
func TestHuawei_ProjectChangeRefreshesCachedPricing(t *testing.T) {
	bss := newProjectBSS(t)
	bss.releaseAll()

	node := flavorNode("c7n.2xlarge.4")
	cfg := &lockedProviderConfig{swappableProviderConfig{fakeProviderConfig: fakeProviderConfig{customPricing: &models.CustomPricing{CPU: "0.01", RAM: "0.001", ProjectID: "proj-a"}}}}
	h := &Huawei{
		Clientset: &clustercache.MockClusterCache{Nodes: []*clustercache.Node{node}},
		Config:    cfg,
		ProjectID: "proj-a",
		Pricing: map[string]*HuaweiPricing{
			"la-south-2,c7n.2xlarge.4,linux": {NodeAttributes: &HuaweiNodeAttributes{Price: "2"}},
		},
	}
	h.lastPricingRefresh.Store(time.Now().UnixNano())
	defer h.refreshes.Wait()

	if _, err := h.UpdateConfigFromConfigMap(map[string]string{"projectID": "proj-b"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	h.refreshes.Wait()

	select {
	case got := <-bss.arrived:
		if got != "proj-b" {
			t.Errorf("expected a demandPrice request for proj-b, got one for %q", got)
		}
	default:
		t.Fatalf("expected the project change to start a download")
	}
	if got := nodePrice(h); got != "3" {
		t.Errorf("expected the node repriced at proj-b's 3, got %q", got)
	}

	// The same project again starts no download.
	if _, err := h.UpdateConfigFromConfigMap(map[string]string{"projectID": "proj-b"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	h.refreshes.Wait()
	select {
	case got := <-bss.arrived:
		t.Errorf("expected no download for an unchanged project, got a request for %q", got)
	default:
	}
}
