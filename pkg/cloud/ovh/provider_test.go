package ovh

import (
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"

	v1 "k8s.io/api/core/v1"
)

func newTestProvider(t *testing.T, filename string) *OVH {
	t.Helper()

	data, err := os.ReadFile(filename)
	if err != nil {
		t.Fatalf("failed to read test fixture %s: %v", filename, err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(data)
	}))
	t.Cleanup(srv.Close)

	provider := &OVH{
		catalogURL: srv.URL,
	}
	if err := provider.DownloadPricingData(); err != nil {
		t.Fatalf("DownloadPricingData failed: %v", err)
	}
	return provider
}

func assertIntEqual(t *testing.T, name string, got, want int) {
	t.Helper()
	if got != want {
		t.Errorf("%s: got %d, want %d", name, got, want)
	}
}

func assertFloatClose(t *testing.T, name string, got, want, tolerance float64) {
	t.Helper()
	if math.Abs(got-want) > tolerance {
		t.Errorf("%s: got %f, want %f (tolerance %f)", name, got, want, tolerance)
	}
}

func parseFloat(t *testing.T, s string) float64 {
	t.Helper()
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		t.Fatalf("parseFloat(%q) failed: %v", s, err)
	}
	return v
}

func TestParseCatalog(t *testing.T) {
	data, err := os.ReadFile("testdata/ovh_catalog.json")
	if err != nil {
		t.Fatalf("failed to read catalog fixture: %v", err)
	}

	pricing, volumePricing, err := parseCatalog(data)
	if err != nil {
		t.Fatalf("parseCatalog failed: %v", err)
	}

	// b2-7 instance: hourly and monthly
	b2, ok := pricing["b2-7"]
	if !ok {
		t.Fatal("b2-7 flavor not found")
	}
	// Hourly: 6810000 microcents / 100_000_000 = 0.0681
	assertFloatClose(t, "b2-7 hourly", b2.HourlyPrice, 0.0681, 0.0001)
	// Monthly: 2420000000 / 100_000_000 / 730 = 24.2 / 730
	assertFloatClose(t, "b2-7 monthly", b2.MonthlyPrice, 24.2/730.0, 0.0001)
	assertIntEqual(t, "b2-7 VCPU", b2.VCPU, 2)
	assertIntEqual(t, "b2-7 RAM", b2.RAM, 7)
	assertIntEqual(t, "b2-7 Disk", b2.Disk, 50)
	assertIntEqual(t, "b2-7 GPU", b2.GPU, 0)

	// t2-45 GPU instance
	t2, ok := pricing["t2-45"]
	if !ok {
		t.Fatal("t2-45 flavor not found")
	}
	// Hourly: 180000000 / 100_000_000 = 1.8
	assertFloatClose(t, "t2-45 hourly", t2.HourlyPrice, 1.8, 0.0001)
	// Monthly: 63800000000 / 100_000_000 / 730 = 638 / 730
	assertFloatClose(t, "t2-45 monthly", t2.MonthlyPrice, 638.0/730.0, 0.0001)
	assertIntEqual(t, "t2-45 VCPU", t2.VCPU, 15)
	assertIntEqual(t, "t2-45 RAM", t2.RAM, 45)
	assertIntEqual(t, "t2-45 Disk", t2.Disk, 400)
	assertIntEqual(t, "t2-45 GPU", t2.GPU, 1)
	if t2.GPUName != "Tesla V100S" {
		t.Errorf("t2-45 GPUName: got %q, want %q", t2.GPUName, "Tesla V100S")
	}

	// Volume pricing
	// high-speed-gen2: 11900 / 100_000_000 = 0.000119
	hsGen2, ok := volumePricing["high-speed-gen2"]
	if !ok {
		t.Fatal("high-speed-gen2 volume type not found")
	}
	assertFloatClose(t, "high-speed-gen2", hsGen2, 0.000119, 0.000001)

	hs, ok := volumePricing["high-speed"]
	if !ok {
		t.Fatal("high-speed volume type not found")
	}
	assertFloatClose(t, "high-speed", hs, 0.000119, 0.000001)

	// classic: 5900 / 100_000_000 = 0.000059
	classic, ok := volumePricing["classic"]
	if !ok {
		t.Fatal("classic volume type not found")
	}
	assertFloatClose(t, "classic", classic, 0.000059, 0.000001)
}

func TestOVHKey(t *testing.T) {
	key := &ovhKey{
		Labels: map[string]string{
			v1.LabelTopologyRegion:     "GRA7",
			v1.LabelInstanceTypeStable: "b2-7",
		},
	}

	if got := key.Features(); got != "GRA7,b2-7" {
		t.Errorf("Features(): got %q, want %q", got, "GRA7,b2-7")
	}
	if got := key.GPUType(); got != "" {
		t.Errorf("GPUType(): got %q, want empty", got)
	}
	if got := key.GPUCount(); got != 0 {
		t.Errorf("GPUCount(): got %d, want 0", got)
	}
	if got := key.ID(); got != "" {
		t.Errorf("ID(): got %q, want empty", got)
	}
}

func TestOVHKeyGPU(t *testing.T) {
	tests := []struct {
		instanceType string
		wantGPU      string
	}{
		{"t2-45", "t2-45"},
		{"l4-24", "l4-24"},
		{"l40s-48", "l40s-48"},
		{"a10-96", "a10-96"},
		{"a100-180", "a100-180"},
		{"b2-7", ""},
		{"d2-4", ""},
	}

	for _, tc := range tests {
		t.Run(tc.instanceType, func(t *testing.T) {
			key := &ovhKey{
				Labels: map[string]string{
					v1.LabelInstanceTypeStable: tc.instanceType,
				},
			}
			if got := key.GPUType(); got != tc.wantGPU {
				t.Errorf("GPUType(%s): got %q, want %q", tc.instanceType, got, tc.wantGPU)
			}
		})
	}
}

func TestOVHPVKey(t *testing.T) {
	tests := []struct {
		name         string
		storageClass string
		zone         string
		wantFeatures string
	}{
		{"high-speed-gen2", "csi-cinder-high-speed-gen2", "GRA7", "GRA7,high-speed-gen2"},
		{"high-speed", "csi-cinder-high-speed", "GRA9", "GRA9,high-speed"},
		{"classic", "csi-cinder-classic", "BHS5", "BHS5,classic"},
		{"unknown", "unknown-class", "GRA7", "GRA7,"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			key := &ovhPVKey{
				StorageClassName: tc.storageClass,
				Zone:             tc.zone,
			}
			if got := key.Features(); got != tc.wantFeatures {
				t.Errorf("Features(): got %q, want %q", got, tc.wantFeatures)
			}
			if got := key.GetStorageClass(); got != tc.storageClass {
				t.Errorf("GetStorageClass(): got %q, want %q", got, tc.storageClass)
			}
			if got := key.ID(); got != "" {
				t.Errorf("ID(): got %q, want empty", got)
			}
		})
	}
}

func TestIsMonthlyBilling(t *testing.T) {
	tests := []struct {
		name         string
		labels       map[string]string
		monthlyPools []string
		want         bool
	}{
		{
			name:   "default hourly",
			labels: map[string]string{},
			want:   false,
		},
		{
			name:   "label monthly",
			labels: map[string]string{BillingLabel: "monthly"},
			want:   true,
		},
		{
			name:   "label hourly",
			labels: map[string]string{BillingLabel: "hourly"},
			want:   false,
		},
		{
			name:         "env monthly",
			labels:       map[string]string{NodepoolLabel: "pool-monthly"},
			monthlyPools: []string{"pool-monthly", "other-pool"},
			want:         true,
		},
		{
			name:         "env miss",
			labels:       map[string]string{NodepoolLabel: "pool-hourly"},
			monthlyPools: []string{"pool-monthly"},
			want:         false,
		},
		{
			name:         "label overrides env",
			labels:       map[string]string{BillingLabel: "hourly", NodepoolLabel: "pool-monthly"},
			monthlyPools: []string{"pool-monthly"},
			want:         false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := isMonthlyBilling(tc.labels, tc.monthlyPools)
			if got != tc.want {
				t.Errorf("isMonthlyBilling(): got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestNodePricing_Hourly(t *testing.T) {
	provider := newTestProvider(t, "testdata/ovh_catalog.json")

	key := &ovhKey{
		Labels: map[string]string{
			v1.LabelTopologyRegion:     "GRA7",
			v1.LabelInstanceTypeStable: "b2-7",
		},
	}

	node, meta, err := provider.NodePricing(key)
	if err != nil {
		t.Fatalf("NodePricing failed: %v", err)
	}

	if meta.Source != "ovh" {
		t.Errorf("Source: got %q, want %q", meta.Source, "ovh")
	}
	assertFloatClose(t, "cost", parseFloat(t, node.Cost), 0.0681, 0.0001)
	assertIntEqual(t, "VCPU", int(parseFloat(t, node.VCPU)), 2)
	assertIntEqual(t, "RAM", int(parseFloat(t, node.RAM)), 7)
	assertIntEqual(t, "Storage", int(parseFloat(t, node.Storage)), 50)
	if node.Region != "GRA7" {
		t.Errorf("Region: got %q, want %q", node.Region, "GRA7")
	}
	if node.InstanceType != "b2-7" {
		t.Errorf("InstanceType: got %q, want %q", node.InstanceType, "b2-7")
	}
}

func TestNodePricing_Monthly(t *testing.T) {
	provider := newTestProvider(t, "testdata/ovh_catalog.json")

	key := &ovhKey{
		Labels: map[string]string{
			v1.LabelTopologyRegion:     "GRA7",
			v1.LabelInstanceTypeStable: "b2-7",
			BillingLabel:               "monthly",
		},
	}

	node, _, err := provider.NodePricing(key)
	if err != nil {
		t.Fatalf("NodePricing failed: %v", err)
	}

	// Monthly price: 24.2 / 730
	assertFloatClose(t, "cost", parseFloat(t, node.Cost), 24.2/730.0, 0.0001)
}

func TestNodePricing_MonthlyViaEnv(t *testing.T) {
	provider := newTestProvider(t, "testdata/ovh_catalog.json")
	provider.monthlyNodepools = []string{"my-monthly-pool"}

	key := &ovhKey{
		Labels: map[string]string{
			v1.LabelTopologyRegion:     "GRA7",
			v1.LabelInstanceTypeStable: "b2-7",
			NodepoolLabel:              "my-monthly-pool",
		},
	}

	node, _, err := provider.NodePricing(key)
	if err != nil {
		t.Fatalf("NodePricing failed: %v", err)
	}

	assertFloatClose(t, "cost", parseFloat(t, node.Cost), 24.2/730.0, 0.0001)
}

func TestNodePricing_GPU(t *testing.T) {
	provider := newTestProvider(t, "testdata/ovh_catalog.json")

	key := &ovhKey{
		Labels: map[string]string{
			v1.LabelTopologyRegion:     "GRA7",
			v1.LabelInstanceTypeStable: "t2-45",
		},
	}

	node, _, err := provider.NodePricing(key)
	if err != nil {
		t.Fatalf("NodePricing failed: %v", err)
	}

	assertFloatClose(t, "cost", parseFloat(t, node.Cost), 1.8, 0.0001)
	assertIntEqual(t, "GPU", int(parseFloat(t, node.GPU)), 1)
	if node.GPUName != "Tesla V100S" {
		t.Errorf("GPUName: got %q, want %q", node.GPUName, "Tesla V100S")
	}
	assertIntEqual(t, "VCPU", int(parseFloat(t, node.VCPU)), 15)
	assertIntEqual(t, "RAM", int(parseFloat(t, node.RAM)), 45)
}

func TestNodePricing_NotFound(t *testing.T) {
	provider := newTestProvider(t, "testdata/ovh_catalog.json")

	key := &ovhKey{
		Labels: map[string]string{
			v1.LabelTopologyRegion:     "GRA7",
			v1.LabelInstanceTypeStable: "unknown-flavor",
		},
	}

	_, _, err := provider.NodePricing(key)
	if err == nil {
		t.Fatal("expected error for unknown flavor, got nil")
	}
}

func TestPVPricing(t *testing.T) {
	provider := newTestProvider(t, "testdata/ovh_catalog.json")

	key := &ovhPVKey{
		StorageClassName: "csi-cinder-high-speed-gen2",
		Zone:             "GRA7",
	}

	pv, err := provider.PVPricing(key)
	if err != nil {
		t.Fatalf("PVPricing failed: %v", err)
	}

	assertFloatClose(t, "cost", parseFloat(t, pv.Cost), 0.000119, 0.000001)
	if pv.Class != "csi-cinder-high-speed-gen2" {
		t.Errorf("Class: got %q, want %q", pv.Class, "csi-cinder-high-speed-gen2")
	}
}

func TestNetworkPricing(t *testing.T) {
	provider := &OVH{}

	net, err := provider.NetworkPricing()
	if err != nil {
		t.Fatalf("NetworkPricing failed: %v", err)
	}

	if net.ZoneNetworkEgressCost != 0 {
		t.Errorf("ZoneNetworkEgressCost: got %f, want 0", net.ZoneNetworkEgressCost)
	}
	if net.RegionNetworkEgressCost != 0 {
		t.Errorf("RegionNetworkEgressCost: got %f, want 0", net.RegionNetworkEgressCost)
	}
	assertFloatClose(t, "InternetNetworkEgressCost", net.InternetNetworkEgressCost, 0.01, 0.0001)
	if net.NatGatewayEgressCost != 0 {
		t.Errorf("NatGatewayEgressCost: got %f, want 0", net.NatGatewayEgressCost)
	}
	if net.NatGatewayIngressCost != 0 {
		t.Errorf("NatGatewayIngressCost: got %f, want 0", net.NatGatewayIngressCost)
	}
}

func TestLoadBalancerPricing(t *testing.T) {
	provider := &OVH{}

	lb, err := provider.LoadBalancerPricing()
	if err != nil {
		t.Fatalf("LoadBalancerPricing failed: %v", err)
	}

	assertFloatClose(t, "LB cost", lb.Cost, 0.012, 0.0001)
}
