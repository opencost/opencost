package linode

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/config"
	v1 "k8s.io/api/core/v1"
)

type fakeConfig struct {
	pricing *models.CustomPricing
}

func (f *fakeConfig) ConfigFileManager() *config.ConfigFileManager { return nil }

func (f *fakeConfig) GetCustomPricingData() (*models.CustomPricing, error) {
	return f.pricing, nil
}

func (f *fakeConfig) Update(fn func(*models.CustomPricing) error) (*models.CustomPricing, error) {
	return f.pricing, fn(f.pricing)
}

func (f *fakeConfig) UpdateFromMap(map[string]string) (*models.CustomPricing, error) {
	return f.pricing, nil
}

func fallbackConfig() *fakeConfig {
	return &fakeConfig{pricing: &models.CustomPricing{
		Storage:               "0.000137",
		DefaultLBPrice:        "0.015",
		ZoneNetworkEgress:     "0.0",
		RegionNetworkEgress:   "0.0",
		InternetNetworkEgress: "0.005",
	}}
}

type fakeAPI struct {
	files       map[string]string
	status      map[string]int
	clusterAuth string
	requests    map[string]int
}

func newFakeAPI() *fakeAPI {
	return &fakeAPI{
		files: map[string]string{
			"/linode/types?1":        "testdata/types_page1.json",
			"/linode/types?2":        "testdata/types_page2.json",
			"/lke/types?1":           "testdata/lke_types.json",
			"/volumes/types?1":       "testdata/volume_types.json",
			"/nodebalancers/types?1": "testdata/nodebalancer_types.json",
			"/regions?1":             "testdata/regions.json",
		},
		status:   map[string]int{},
		requests: map[string]int{},
	}
}

func (a *fakeAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.requests[r.URL.Path]++
	if code, ok := a.status[r.URL.Path]; ok {
		w.WriteHeader(code)
		return
	}

	if r.URL.Path == "/lke/clusters/12345" {
		a.clusterAuth = r.Header.Get("Authorization")
		w.Write([]byte(`{"id": 12345, "control_plane": {"high_availability": true}}`))
		return
	}

	if r.URL.Query().Get("page_size") != strconv.Itoa(apiPageSize) {
		http.Error(w, "unexpected page_size", http.StatusBadRequest)
		return
	}
	file, ok := a.files[r.URL.Path+"?"+r.URL.Query().Get("page")]
	if !ok {
		http.NotFound(w, r)
		return
	}
	data, err := os.ReadFile(file)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func newTestProvider(t *testing.T, api *fakeAPI, l *Linode) *Linode {
	t.Helper()
	t.Setenv("LINODE_TOKEN", "")
	t.Setenv("CLOUD_PROVIDER_API_KEY", "")

	srv := httptest.NewServer(api)
	t.Cleanup(srv.Close)

	l.apiURL = srv.URL
	if l.Config == nil {
		l.Config = fallbackConfig()
	}
	if err := l.DownloadPricingData(); err != nil {
		t.Fatalf("DownloadPricingData: %v", err)
	}
	return l
}

func nodeKey(region, instanceType string) models.Key {
	return &linodeKey{Labels: map[string]string{
		v1.LabelTopologyRegion:     region,
		v1.LabelInstanceTypeStable: instanceType,
	}}
}

func TestParseLKEClusterID(t *testing.T) {
	tests := map[string]string{
		"lke12345-67890-0a1b2c3d4e": "12345",
		"lke1-2-abc":                "1",
		"lkeabc-1-2":                "",
		"gke-cluster-pool-1":        "",
		"":                          "",
	}
	for name, want := range tests {
		if got := ParseLKEClusterID(name); got != want {
			t.Errorf("ParseLKEClusterID(%q) = %q, want %q", name, got, want)
		}
	}
}

func TestDownloadPricingDataFollowsPages(t *testing.T) {
	api := newFakeAPI()
	l := newTestProvider(t, api, &Linode{})

	for _, id := range []string{"g6-standard-2", "g6-dedicated-4", "g1-gpu-rtx6000-1"} {
		if _, ok := l.InstanceTypes[id]; !ok {
			t.Errorf("instance type %s not loaded", id)
		}
	}
	if api.requests["/linode/types"] != 2 {
		t.Errorf("expected 2 requests for /linode/types, got %d", api.requests["/linode/types"])
	}
	for _, id := range []string{lkeStandardID, lkeHAID, volumeID, nodeBalancerID} {
		if _, ok := l.ProductTypes[id]; !ok {
			t.Errorf("product type %s not loaded", id)
		}
	}
	if got := len(l.Regions()); got != 3 {
		t.Errorf("Regions(): got %d regions, want 3", got)
	}
}

func TestDownloadPricingDataFailsWithoutInstanceTypes(t *testing.T) {
	api := newFakeAPI()
	api.status["/linode/types"] = http.StatusInternalServerError

	srv := httptest.NewServer(api)
	defer srv.Close()

	l := &Linode{apiURL: srv.URL, Config: fallbackConfig()}
	if err := l.DownloadPricingData(); err == nil {
		t.Fatal("expected an error when /linode/types fails")
	}
	if l.PricingSourceStatus()[LinodeAPIPricing].Available {
		t.Error("pricing source should be unavailable without instance types")
	}
}

func TestNodePricing(t *testing.T) {
	l := newTestProvider(t, newFakeAPI(), &Linode{})

	tests := []struct {
		name, region, instanceType string
		cost, vcpu, ramBytes, gpu  string
	}{
		{"base price", "us-east", "g6-standard-2", "0.036", "2", "4294967296", ""},
		{"region price", "br-gru", "g6-standard-2", "0.05", "2", "4294967296", ""},
		{"dedicated", "eu-west", "g6-dedicated-4", "0.108", "4", "8589934592", ""},
		{"gpu", "us-east", "g1-gpu-rtx6000-1", "1.5", "8", "34359738368", "1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node, meta, err := l.NodePricing(nodeKey(tt.region, tt.instanceType))
			if err != nil {
				t.Fatalf("NodePricing: %v", err)
			}
			if node.Cost != tt.cost {
				t.Errorf("Cost: got %s, want %s", node.Cost, tt.cost)
			}
			if node.VCPU != tt.vcpu {
				t.Errorf("VCPU: got %s, want %s", node.VCPU, tt.vcpu)
			}
			if node.RAMBytes != tt.ramBytes {
				t.Errorf("RAMBytes: got %s, want %s", node.RAMBytes, tt.ramBytes)
			}
			if node.GPU != tt.gpu {
				t.Errorf("GPU: got %q, want %q", node.GPU, tt.gpu)
			}
			if node.InstanceType != tt.instanceType || node.Region != tt.region {
				t.Errorf("got %s in %s, want %s in %s", node.InstanceType, node.Region, tt.instanceType, tt.region)
			}
			if node.PricingType != models.DefaultPrices {
				t.Errorf("PricingType: got %v, want %v", node.PricingType, models.DefaultPrices)
			}
			if meta.Currency != "USD" {
				t.Errorf("Currency: got %s, want USD", meta.Currency)
			}
		})
	}
}

func TestNodePricingUnknownType(t *testing.T) {
	l := newTestProvider(t, newFakeAPI(), &Linode{})

	if _, _, err := l.NodePricing(nodeKey("us-east", "g6-unknown-1")); err == nil {
		t.Error("expected an error for an unknown type")
	}
	if _, _, err := l.NodePricing(nodeKey("us-east", "")); err == nil {
		t.Error("expected an error for a node without an instance type")
	}
}

func TestKeyFallsBackToLinodeRegionLabel(t *testing.T) {
	l := &Linode{}
	key := l.GetKey(map[string]string{
		RegionLabel:                "us-east",
		v1.LabelInstanceTypeStable: "g6-standard-2",
	}, &clustercache.Node{})

	if got := key.Features(); got != "us-east,g6-standard-2" {
		t.Errorf("Features(): got %q", got)
	}
}

func TestPVPricing(t *testing.T) {
	l := newTestProvider(t, newFakeAPI(), &Linode{ClusterRegion: "us-east"})

	pv := &clustercache.PersistentVolume{Spec: v1.PersistentVolumeSpec{
		StorageClassName: "linode-block-storage-retain",
		PersistentVolumeSource: v1.PersistentVolumeSource{
			CSI: &v1.CSIPersistentVolumeSource{VolumeHandle: "123456-pvc0a1b2c3d"},
		},
		NodeAffinity: &v1.VolumeNodeAffinity{Required: &v1.NodeSelector{
			NodeSelectorTerms: []v1.NodeSelectorTerm{{MatchExpressions: []v1.NodeSelectorRequirement{{
				Key: RegionLabel, Operator: v1.NodeSelectorOpIn, Values: []string{"br-gru"},
			}}}},
		}},
	}}

	key := l.GetPVKey(pv, nil, "")
	if key.ID() != "123456-pvc0a1b2c3d" {
		t.Errorf("ID(): got %q", key.ID())
	}

	got, err := l.PVPricing(key)
	if err != nil {
		t.Fatalf("PVPricing: %v", err)
	}
	if got.Cost != "0.00021" || got.Region != "br-gru" || got.Class != "linode-block-storage-retain" {
		t.Errorf("got %+v", got)
	}

	pv.Spec.NodeAffinity = nil
	got, err = l.PVPricing(l.GetPVKey(pv, nil, ""))
	if err != nil {
		t.Fatalf("PVPricing: %v", err)
	}
	if got.Cost != "0.00015" || got.Region != "us-east" {
		t.Errorf("cluster region volume: got %+v", got)
	}
}

func TestPricesFallBackToConfig(t *testing.T) {
	api := newFakeAPI()
	api.status["/volumes/types"] = http.StatusServiceUnavailable
	api.status["/nodebalancers/types"] = http.StatusServiceUnavailable
	l := newTestProvider(t, api, &Linode{ClusterRegion: "us-east"})

	pv, err := l.PVPricing(&linodePVKey{Region: "us-east"})
	if err != nil {
		t.Fatalf("PVPricing: %v", err)
	}
	if pv.Cost != "0.000137" {
		t.Errorf("PV cost: got %s, want config fallback 0.000137", pv.Cost)
	}

	lb, err := l.LoadBalancerPricing()
	if err != nil {
		t.Fatalf("LoadBalancerPricing: %v", err)
	}
	if lb.Cost != 0.015 {
		t.Errorf("LB cost: got %v, want config fallback 0.015", lb.Cost)
	}
}

func TestLoadBalancerPricing(t *testing.T) {
	l := newTestProvider(t, newFakeAPI(), &Linode{ClusterRegion: "br-gru"})

	lb, err := l.LoadBalancerPricing()
	if err != nil {
		t.Fatalf("LoadBalancerPricing: %v", err)
	}
	if lb.Cost != 0.021 {
		t.Errorf("got %v, want 0.021", lb.Cost)
	}
}

func TestNetworkPricing(t *testing.T) {
	l := &Linode{Config: fallbackConfig()}

	n, err := l.NetworkPricing()
	if err != nil {
		t.Fatalf("NetworkPricing: %v", err)
	}
	if n.ZoneNetworkEgressCost != 0 || n.RegionNetworkEgressCost != 0 || n.InternetNetworkEgressCost != 0.005 {
		t.Errorf("got %+v", n)
	}

	l.Config.(*fakeConfig).pricing.InternetNetworkEgress = "free"
	if _, err := l.NetworkPricing(); err == nil {
		t.Error("expected an error for an invalid egress price")
	}
}

func TestClusterManagementPricing(t *testing.T) {
	t.Run("standard control plane without a token", func(t *testing.T) {
		api := newFakeAPI()
		l := newTestProvider(t, api, &Linode{ClusterRegion: "us-east", LKEClusterID: "12345"})

		if api.requests["/lke/clusters/12345"] != 0 {
			t.Error("cluster looked up without a token")
		}
		provisioner, price, err := l.ClusterManagementPricing()
		if err != nil || provisioner != "LKE" || price != 0 {
			t.Errorf("got (%q, %v, %v), want (LKE, 0, nil)", provisioner, price, err)
		}
	})

	t.Run("high availability control plane", func(t *testing.T) {
		api := newFakeAPI()
		l := newTestProvider(t, api, &Linode{ClusterRegion: "br-gru", LKEClusterID: "12345", apiToken: "secret"})

		if api.clusterAuth != "Bearer secret" {
			t.Errorf("Authorization: got %q", api.clusterAuth)
		}
		_, price, err := l.ClusterManagementPricing()
		if err != nil || price != 0.126 {
			t.Errorf("got (%v, %v), want (0.126, nil)", price, err)
		}
	})

	t.Run("cluster lookup failure", func(t *testing.T) {
		api := newFakeAPI()
		api.status["/lke/clusters/12345"] = http.StatusUnauthorized
		l := newTestProvider(t, api, &Linode{ClusterRegion: "us-east", LKEClusterID: "12345", apiToken: "bad"})

		_, price, err := l.ClusterManagementPricing()
		if err != nil || price != 0 {
			t.Errorf("got (%v, %v), want (0, nil)", price, err)
		}
	})
}

func TestRegionsOverride(t *testing.T) {
	t.Setenv("REGION_OVERRIDE_LIST", "us-east,us-west")
	l := &Linode{RegionList: []string{"eu-west"}}

	got := l.Regions()
	if len(got) != 2 || got[0] != "us-east" || got[1] != "us-west" {
		t.Errorf("Regions(): got %v", got)
	}
}
