package huawei

import (
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
