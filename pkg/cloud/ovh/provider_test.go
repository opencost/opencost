package ovh

import (
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"

	"github.com/opencost/opencost/core/pkg/clustercache"
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

	pricing, volumePricing, lbPricing, err := parseCatalog(data)
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

	// Load balancer pricing
	lbS, ok := lbPricing["small"]
	if !ok {
		t.Fatal("small load balancer flavor not found")
	}
	assertFloatClose(t, "lb small hourly", lbS.HourlyPrice, 0.0083, 0.0001)
	assertFloatClose(t, "lb small monthly", lbS.MonthlyPrice, 6.0/730.0, 0.0001)

	lbM, ok := lbPricing["medium"]
	if !ok {
		t.Fatal("medium load balancer flavor not found")
	}
	assertFloatClose(t, "lb medium hourly", lbM.HourlyPrice, 0.0208, 0.0001)
	assertFloatClose(t, "lb medium monthly", lbM.MonthlyPrice, 15.0/730.0, 0.0001)

	lbL, ok := lbPricing["large"]
	if !ok {
		t.Fatal("large load balancer flavor not found")
	}
	assertFloatClose(t, "lb large hourly", lbL.HourlyPrice, 0.0556, 0.0001)
	assertFloatClose(t, "lb large monthly", lbL.MonthlyPrice, 40.0/730.0, 0.0001)

	lbXL, ok := lbPricing["xl"]
	if !ok {
		t.Fatal("xl load balancer flavor not found")
	}
	assertFloatClose(t, "lb xl hourly", lbXL.HourlyPrice, 0.2083, 0.0001)
	assertFloatClose(t, "lb xl monthly", lbXL.MonthlyPrice, 150.0/730.0, 0.0001)
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
	t.Run("default fallback without catalog", func(t *testing.T) {
		provider := &OVH{}

		lb, err := provider.LoadBalancerPricing()
		if err != nil {
			t.Fatalf("LoadBalancerPricing failed: %v", err)
		}

		// Defaults to small flavor fallback price
		assertFloatClose(t, "default fallback LB cost", lb.Cost, 0.0083, 0.0001)
	})

	t.Run("flavor-based pricing from catalog", func(t *testing.T) {
		provider := newTestProvider(t, "testdata/ovh_catalog.json")

		testCases := []struct {
			name        string
			service     *clustercache.Service
			wantCost    float64
			description string
		}{
			{
				name:        "nil service defaults to small",
				service:     nil,
				wantCost:    0.0083,
				description: "default small",
			},
			{
				name: "unannotated service defaults to small",
				service: &clustercache.Service{
					Name:      "test-lb-default",
					Namespace: "default",
					Type:      v1.ServiceTypeLoadBalancer,
				},
				wantCost:    0.0083,
				description: "unannotated service",
			},
			{
				name: "MKS Free small flavor annotation",
				service: &clustercache.Service{
					Name:      "test-lb-s",
					Namespace: "default",
					Type:      v1.ServiceTypeLoadBalancer,
					Annotations: map[string]string{
						"loadbalancer.ovhcloud.com/flavor": "small",
					},
				},
				wantCost:    0.0083,
				description: "small flavor",
			},
			{
				name: "MKS Free short flavor 's'",
				service: &clustercache.Service{
					Name:      "test-lb-s-short",
					Namespace: "default",
					Type:      v1.ServiceTypeLoadBalancer,
					Annotations: map[string]string{
						"loadbalancer.ovhcloud.com/flavor": "s",
					},
				},
				wantCost:    0.0083,
				description: "'s' flavor alias",
			},
			{
				name: "MKS Free medium flavor annotation",
				service: &clustercache.Service{
					Name:      "test-lb-m",
					Namespace: "default",
					Type:      v1.ServiceTypeLoadBalancer,
					Annotations: map[string]string{
						"loadbalancer.ovhcloud.com/flavor": "medium",
					},
				},
				wantCost:    0.0208,
				description: "medium flavor",
			},
			{
				name: "MKS Free large flavor annotation",
				service: &clustercache.Service{
					Name:      "test-lb-l",
					Namespace: "default",
					Type:      v1.ServiceTypeLoadBalancer,
					Annotations: map[string]string{
						"loadbalancer.ovhcloud.com/flavor": "large",
					},
				},
				wantCost:    0.0556,
				description: "large flavor",
			},
			{
				name: "MKS Free xl flavor annotation",
				service: &clustercache.Service{
					Name:      "test-lb-xl",
					Namespace: "default",
					Type:      v1.ServiceTypeLoadBalancer,
					Annotations: map[string]string{
						"loadbalancer.ovhcloud.com/flavor": "xl",
					},
				},
				wantCost:    0.2083,
				description: "xl flavor",
			},
			{
				name: "unknown flavor annotation falls back to small",
				service: &clustercache.Service{
					Name:      "test-lb-unknown-flavor",
					Namespace: "default",
					Type:      v1.ServiceTypeLoadBalancer,
					Annotations: map[string]string{
						"loadbalancer.ovhcloud.com/flavor": "unknown-custom-flavor",
					},
				},
				wantCost:    0.0083,
				description: "unknown flavor fallback to small",
			},
			{
				name: "unrelated annotations fall back to small",
				service: &clustercache.Service{
					Name:      "test-lb-unrelated-annotations",
					Namespace: "default",
					Type:      v1.ServiceTypeLoadBalancer,
					Annotations: map[string]string{
						"example.com/some-annotation": "value",
					},
				},
				wantCost:    0.0083,
				description: "unrelated annotations fallback to small",
			},
			{
				name: "monthly billing annotation",
				service: &clustercache.Service{
					Name:      "test-lb-monthly",
					Namespace: "default",
					Type:      v1.ServiceTypeLoadBalancer,
					Annotations: map[string]string{
						"loadbalancer.ovhcloud.com/flavor": "medium",
						"ovh.opencost.io/billing":          "monthly",
					},
				},
				wantCost:    15.0 / 730.0,
				description: "monthly billing medium",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				lb, err := provider.ServiceLoadBalancerPricing(tc.service)
				if err != nil {
					t.Fatalf("ServiceLoadBalancerPricing failed: %v", err)
				}
				assertFloatClose(t, tc.description, lb.Cost, tc.wantCost, 0.0001)
			})
		}
	})

	t.Run("regression test for incorrect fixed 0.012 pricing", func(t *testing.T) {
		provider := newTestProvider(t, "testdata/ovh_catalog.json")

		flavors := []struct {
			flavor   string
			expected float64
		}{
			{"small", 0.0083},
			{"medium", 0.0208},
			{"large", 0.0556},
			{"xl", 0.2083},
		}

		costs := make(map[string]float64)
		for _, f := range flavors {
			svc := &clustercache.Service{
				Name:      "svc-" + f.flavor,
				Namespace: "default",
				Type:      v1.ServiceTypeLoadBalancer,
				Annotations: map[string]string{
					"loadbalancer.ovhcloud.com/flavor": f.flavor,
				},
			}
			lb, err := provider.ServiceLoadBalancerPricing(svc)
			if err != nil {
				t.Fatalf("ServiceLoadBalancerPricing(%s) failed: %v", f.flavor, err)
			}
			costs[f.flavor] = lb.Cost

			// Verify it does NOT equal the old fixed 0.012 cost
			if math.Abs(lb.Cost-0.012) < 0.0001 {
				t.Errorf("flavor %s received old hardcoded 0.012 cost", f.flavor)
			}
			// Verify it matches expected catalog pricing
			assertFloatClose(t, f.flavor, lb.Cost, f.expected, 0.0001)
		}

		// Ensure different flavors have distinct prices
		if costs["small"] == costs["medium"] || costs["medium"] == costs["large"] || costs["large"] == costs["xl"] {
			t.Errorf("different flavors must not have identical costs: %+v", costs)
		}
	})
}

func TestExtractLBFlavor(t *testing.T) {
	testCases := []struct {
		name     string
		service  *clustercache.Service
		expected string
	}{
		{
			name:     "nil service",
			service:  nil,
			expected: "",
		},
		{
			name: "service without annotations",
			service: &clustercache.Service{
				Name: "svc-no-annotations",
			},
			expected: "",
		},
		{
			name: "valid flavor annotation",
			service: &clustercache.Service{
				Annotations: map[string]string{
					"loadbalancer.ovhcloud.com/flavor": "medium",
				},
			},
			expected: "medium",
		},
		{
			name: "unrelated annotations return empty",
			service: &clustercache.Service{
				Annotations: map[string]string{
					"example.com/unrelated": "test",
				},
			},
			expected: "",
		},
		{
			name: "labels are not used for flavor detection",
			service: &clustercache.Service{
				Labels: map[string]string{
					"loadbalancer.ovhcloud.com/flavor": "large",
				},
			},
			expected: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := extractLBFlavor(tc.service)
			if got != tc.expected {
				t.Errorf("extractLBFlavor() = %q, want %q", got, tc.expected)
			}
		})
	}
}
