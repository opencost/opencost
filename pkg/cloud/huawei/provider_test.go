package huawei

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/pkg/cloud/models"
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
	h := &Huawei{}
	lb, err := h.LoadBalancerPricing()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if lb.Cost != 0.03 {
		t.Fatalf("expected cost 0.03, got %v", lb.Cost)
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
	if _, err := h.UpdateConfig(nil, "customPricing"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := h.UpdateConfigFromConfigMap(map[string]string{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
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
				{"id": "pv-1", "amount": "0.700"}
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
}

// TestHuawei_DownloadPricingData_MissingCredentials verifies that a missing
// AK/SK/domain ID degrades gracefully to the static fallback instead of failing
// DownloadPricingData outright.
func TestHuawei_DownloadPricingData_MissingCredentials(t *testing.T) {
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
	requested := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requested = true
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
	if requested {
		t.Fatalf("expected no BSS request when no PV/node could be confidently priced")
	}
	if len(h.Pricing) != 0 {
		t.Fatalf("expected no live pricing for an unmapped storage class, got %+v", h.Pricing)
	}
}
