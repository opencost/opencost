package huawei

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/env"
)

const testCCEClusterID = "c0ffee00-1234-5678-9abc-def012345678"

func gpuNode(gpus string, labels map[string]string) *clustercache.Node {
	capacity := v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse("8"),
		v1.ResourceMemory: resource.MustParse("32Gi"),
	}
	if gpus != "" {
		capacity[gpuResourceName] = resource.MustParse(gpus)
	}
	return &clustercache.Node{Labels: labels, Status: v1.NodeStatus{Capacity: capacity}}
}

func TestGetKey_GPU(t *testing.T) {
	h := &Huawei{}

	labels := map[string]string{"accelerator": "nvidia-t4", "nvidia.com/gpu.product": "Tesla-T4"}
	k := h.GetKey(labels, gpuNode("1", labels))
	if k.GPUCount() != 1 || k.GPUType() != "nvidia-t4" {
		t.Fatalf("expected 1 nvidia-t4 GPU, got %d %q", k.GPUCount(), k.GPUType())
	}

	gfd := map[string]string{"nvidia.com/gpu.product": "Tesla-T4"}
	if k := h.GetKey(gfd, gpuNode("2", gfd)); k.GPUCount() != 2 || k.GPUType() != "Tesla-T4" {
		t.Fatalf("expected 2 Tesla-T4 GPUs from the GFD label, got %d %q", k.GPUCount(), k.GPUType())
	}

	if k := h.GetKey(labels, gpuNode("", labels)); k.GPUCount() != 0 || k.GPUType() != "" {
		t.Fatalf("expected no GPU on a CPU node despite the label, got %d %q", k.GPUCount(), k.GPUType())
	}
}

func TestNodePricing_GPU(t *testing.T) {
	labels := map[string]string{
		v1.LabelTopologyRegion:     "la-south-2",
		v1.LabelInstanceTypeStable: "pi2.2xlarge.4",
		v1.LabelOSStable:           "linux",
		"accelerator":              "nvidia-t4",
	}
	h := &Huawei{Config: &fakeProviderConfig{customPricing: &models.CustomPricing{}}}
	key := h.GetKey(labels, gpuNode("1", labels))
	h.Pricing = map[string]*HuaweiPricing{
		key.Features(): {NodeAttributes: &HuaweiNodeAttributes{Type: "pi2.2xlarge.4", Price: "1.2"}},
	}
	h.lastPricingRefresh.Store(time.Now().UnixNano())

	node, _, err := h.NodePricing(key)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if node.Cost != "1.2" || node.GPU != "1" || node.GPUName != "nvidia-t4" {
		t.Fatalf("expected the flavor price with 1 nvidia-t4 GPU, got cost %q gpu %q name %q", node.Cost, node.GPU, node.GPUName)
	}
}

func TestClusterManagementPricing(t *testing.T) {
	h := newBilledTestProvider(t)
	h.bills.hourly = map[string]float64{testCCEClusterID: 0.35}
	h.bills.nextFetch = time.Now().Add(time.Hour)

	if name, cost, err := h.ClusterManagementPricing(); err != nil || name != "" || cost != 0 {
		t.Fatalf("expected no cluster management cost without a cluster ID, got %q %v %v", name, cost, err)
	}

	t.Setenv(env.HuaweiCCEClusterIDEnvVar, testCCEClusterID)
	name, cost, err := h.ClusterManagementPricing()
	if err != nil || name != cceProvisionerName || cost != 0.35 {
		t.Fatalf("expected CCE at the billed 0.35/h, got %q %v %v", name, cost, err)
	}
}

// billingServer stands in for BSS cost-analysed-bills with a fixed body.
func billingServer(t *testing.T, body string) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)

	bssEndpointOverride = server.URL
	t.Cleanup(func() { bssEndpointOverride = "" })

	t.Setenv(env.HuaweiAccessKeyIDEnvVar, "test-ak")
	t.Setenv(env.HuaweiAccessKeySecretEnvVar, "test-sk")
	t.Setenv(env.HuaweiDomainIDEnvVar, "test-domain")
}

// TestClusterManagementPricing_KeepsCostOnEmptyBilling covers a refresh that
// reads an empty billing history: the last known cost must survive it rather
// than the cluster dropping to 0 until the next refresh.
func TestClusterManagementPricing_KeepsCostOnEmptyBilling(t *testing.T) {
	billingServer(t, `{"total_count": 0, "cost_data": []}`)
	t.Setenv(env.HuaweiCCEClusterIDEnvVar, testCCEClusterID)

	h := newBilledTestProvider(t)
	h.bills.hourly = map[string]float64{testCCEClusterID: 0.35}
	h.bills.nextFetch = time.Now().Add(-time.Minute)

	if _, cost, err := h.ClusterManagementPricing(); err != nil || cost != 0.35 {
		t.Fatalf("expected the cached 0.35/h while refreshing, got %v %v", cost, err)
	}
	h.bills.refreshes.Wait()

	_, cost, err := h.ClusterManagementPricing()
	if err != nil || cost != 0.35 {
		t.Fatalf("expected the last known 0.35/h to be kept, got %v %v", cost, err)
	}
	if until := time.Until(h.bills.nextFetch); until > billRetryInterval || until <= 0 {
		t.Fatalf("expected a retry within %v, next fetch in %v", billRetryInterval, until)
	}
}

// TestClusterManagementPricing_BareResourceID covers a bill whose RESOURCE_ID
// is the bare cluster ID: the service dimension identifies it as CCE.
func TestClusterManagementPricing_BareResourceID(t *testing.T) {
	today := time.Now().UTC()
	day := func(n int) string { return today.AddDate(0, 0, -n).Format(bssDateLayout) }
	billingServer(t, `{"total_count": 1, "cost_data": [{"dimensions": [
		{"key": "RESOURCE_ID", "value": "`+testCCEClusterID+`"},
		{"key": "CLOUD_SERVICE_TYPE", "value": "Cloud Container Engine"}
	], "costs": [
		{"time_dimension_value": "`+day(3)+`", "amount": "1.00"},
		{"time_dimension_value": "`+day(2)+`", "amount": "4.80"},
		{"time_dimension_value": "`+day(1)+`", "amount": "4.80"}
	]}]}`)
	t.Setenv(env.HuaweiCCEClusterIDEnvVar, testCCEClusterID)

	h := newBilledTestProvider(t)
	h.ClusterManagementPricing()
	h.bills.refreshes.Wait()
	_, cost, err := h.ClusterManagementPricing()
	// (4.80 + 4.80) / (2 days * 24h); the first, partial day is dropped.
	if err != nil || !approxEqual(cost, 0.2) {
		t.Fatalf("expected 0.2/h for the bare cluster ID, got %v %v", cost, err)
	}
}

// TestClusterManagementPricing_RefreshesInBackground checks the metrics path
// never waits on BSS: while a refresh is in flight, ClusterManagementPricing
// answers from the cache. The refresh asks BSS for CCE costs only.
func TestClusterManagementPricing_RefreshesInBackground(t *testing.T) {
	release := make(chan struct{})
	requests := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		requests <- string(body)
		<-release
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"total_count": 0, "cost_data": []}`))
	}))
	t.Cleanup(server.Close)
	bssEndpointOverride = server.URL
	t.Cleanup(func() { bssEndpointOverride = "" })
	t.Setenv(env.HuaweiAccessKeyIDEnvVar, "test-ak")
	t.Setenv(env.HuaweiAccessKeySecretEnvVar, "test-sk")
	t.Setenv(env.HuaweiDomainIDEnvVar, "test-domain")
	t.Setenv(env.HuaweiCCEClusterIDEnvVar, testCCEClusterID)

	h := newBilledTestProvider(t)
	h.bills.hourly = map[string]float64{testCCEClusterID: 0.35}

	done := make(chan float64)
	go func() {
		_, cost, _ := h.ClusterManagementPricing()
		done <- cost
	}()
	select {
	case cost := <-done:
		if cost != 0.35 {
			t.Errorf("expected the cached 0.35/h, got %v", cost)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("ClusterManagementPricing waited on the BSS read")
	}

	requestBody := <-requests
	close(release)
	h.bills.refreshes.Wait()
	for _, want := range []string{`"filters"`, `"CLOUD_SERVICE_TYPE"`, `"hws.service.type.cce"`} {
		if !strings.Contains(requestBody, want) {
			t.Errorf("expected the billing read to filter on the CCE service, missing %s in %s", want, requestBody)
		}
	}
}
