package test

import (
	"fmt"
	"golang.org/x/exp/slices"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/opencost/opencost/pkg/cloud"
	"github.com/opencost/opencost/pkg/clustercache"
	"github.com/opencost/opencost/pkg/config"
	"github.com/opencost/opencost/pkg/costmodel"
	"github.com/opencost/opencost/pkg/costmodel/clusters"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	providerIDMap = "spec.providerID"
	nameMap       = "metadata.name"
	labelMapFoo   = "metadata.labels.foo"
)

func TestRegionValueFromMapField(t *testing.T) {
	wantRegion := "useast"
	wantpid := strings.ToLower("/subscriptions/0bd50fdf-c923-4e1e-850c-196dd3dcc5d3/resourceGroups/MC_test_test_eastus/providers/Microsoft.Compute/virtualMachines/aks-agentpool-20139558-0")
	providerIDWant := wantRegion + "," + wantpid

	n := &v1.Node{}
	n.Spec.ProviderID = "azure:///subscriptions/0bd50fdf-c923-4e1e-850c-196dd3dcc5d3/resourceGroups/MC_test_test_eastus/providers/Microsoft.Compute/virtualMachines/aks-agentpool-20139558-0"
	n.Labels = make(map[string]string)
	n.Labels[v1.LabelZoneRegion] = wantRegion
	got := cloud.NodeValueFromMapField(providerIDMap, n, true)
	if got != providerIDWant {
		t.Errorf("Assert on '%s' want '%s' got '%s'", providerIDMap, providerIDWant, got)
	}

}
func TestTransformedValueFromMapField(t *testing.T) {
	providerIDWant := "i-05445591e0d182d42"
	n := &v1.Node{}
	n.Spec.ProviderID = "aws:///us-east-1a/i-05445591e0d182d42"
	got := cloud.NodeValueFromMapField(providerIDMap, n, false)
	if got != providerIDWant {
		t.Errorf("Assert on '%s' want '%s' got '%s'", providerIDMap, providerIDWant, got)
	}

	providerIDWant2 := strings.ToLower("/subscriptions/0bd50fdf-c923-4e1e-850c-196dd3dcc5d3/resourceGroups/MC_test_test_eastus/providers/Microsoft.Compute/virtualMachines/aks-agentpool-20139558-0")
	n2 := &v1.Node{}
	n2.Spec.ProviderID = "azure:///subscriptions/0bd50fdf-c923-4e1e-850c-196dd3dcc5d3/resourceGroups/MC_test_test_eastus/providers/Microsoft.Compute/virtualMachines/aks-agentpool-20139558-0"
	got2 := cloud.NodeValueFromMapField(providerIDMap, n2, false)
	if got2 != providerIDWant2 {
		t.Errorf("Assert on '%s' want '%s' got '%s'", providerIDMap, providerIDWant2, got2)
	}

	providerIDWant3 := strings.ToLower("/subscriptions/0bd50fdf-c923-4e1e-850c-196dd3dcc5d3/resourceGroups/mc_testspot_testspot_eastus/providers/Microsoft.Compute/virtualMachineScaleSets/aks-nodepool1-19213364-vmss/virtualMachines/0")
	n3 := &v1.Node{}
	n3.Spec.ProviderID = "azure:///subscriptions/0bd50fdf-c923-4e1e-850c-196dd3dcc5d3/resourceGroups/mc_testspot_testspot_eastus/providers/Microsoft.Compute/virtualMachineScaleSets/aks-nodepool1-19213364-vmss/virtualMachines/0"
	got3 := cloud.NodeValueFromMapField(providerIDMap, n3, false)
	if got3 != providerIDWant3 {
		t.Errorf("Assert on '%s' want '%s' got '%s'", providerIDMap, providerIDWant3, got3)
	}
}

func TestNodeValueFromMapField(t *testing.T) {
	providerIDWant := "providerid"
	nameWant := "gke-standard-cluster-1-pool-1-91dc432d-cg69"
	labelFooWant := "labelfoo"

	n := &v1.Node{}
	n.Spec.ProviderID = providerIDWant
	n.Name = nameWant
	n.Labels = make(map[string]string)
	n.Labels["foo"] = labelFooWant

	got := cloud.NodeValueFromMapField(providerIDMap, n, false)
	if got != providerIDWant {
		t.Errorf("Assert on '%s' want '%s' got '%s'", providerIDMap, providerIDWant, got)
	}

	got = cloud.NodeValueFromMapField(nameMap, n, false)
	if got != nameWant {
		t.Errorf("Assert on '%s' want '%s' got '%s'", nameMap, nameWant, got)
	}

	got = cloud.NodeValueFromMapField(labelMapFoo, n, false)
	if got != labelFooWant {
		t.Errorf("Assert on '%s' want '%s' got '%s'", labelMapFoo, labelFooWant, got)
	}

}

func TestPVPriceFromCSV(t *testing.T) {
	nameWant := "pvc-08e1f205-d7a9-4430-90fc-7b3965a18c4d"
	pv := &v1.PersistentVolume{}
	pv.Name = nameWant

	confMan := config.NewConfigFileManager(&config.ConfigFileManagerOpts{
		LocalConfigPath: "./",
	})

	wantPrice := "0.1337"
	c := &cloud.CSVProvider{
		CSVLocation: "../configs/pricing_schema_pv.csv",
		CustomProvider: &cloud.CustomProvider{
			Config: cloud.NewProviderConfig(confMan, "../configs/default.json"),
		},
	}
	c.DownloadPricingData()
	k := c.GetPVKey(pv, make(map[string]string), "")
	resPV, err := c.PVPricing(k)
	if err != nil {
		t.Errorf("Error in NodePricing: %s", err.Error())
	} else {
		gotPrice := resPV.Cost
		if gotPrice != wantPrice {
			t.Errorf("Wanted price '%s' got price '%s'", wantPrice, gotPrice)
		}
	}

}

func TestNodePriceFromCSVWithGPU(t *testing.T) {
	providerIDWant := "providerid"
	nameWant := "gke-standard-cluster-1-pool-1-91dc432d-cg69"
	labelFooWant := "labelfoo"
	wantGPU := "2"

	confMan := config.NewConfigFileManager(&config.ConfigFileManagerOpts{
		LocalConfigPath: "./",
	})

	n := &v1.Node{}
	n.Spec.ProviderID = providerIDWant
	n.Name = nameWant
	n.Labels = make(map[string]string)
	n.Labels["foo"] = labelFooWant
	n.Labels["nvidia.com/gpu_type"] = "Quadro_RTX_4000"
	n.Status.Capacity = v1.ResourceList{"nvidia.com/gpu": *resource.NewScaledQuantity(2, 0)}
	wantPrice := "1.633700"

	n2 := &v1.Node{}
	n2.Spec.ProviderID = providerIDWant
	n2.Name = nameWant
	n2.Labels = make(map[string]string)
	n2.Labels["foo"] = labelFooWant
	n2.Labels["gpu.nvidia.com/class"] = "Quadro_RTX_4001"
	n2.Status.Capacity = v1.ResourceList{"nvidia.com/gpu": *resource.NewScaledQuantity(2, 0)}
	wantPrice2 := "1.733700"

	c := &cloud.CSVProvider{
		CSVLocation: "../configs/pricing_schema.csv",
		CustomProvider: &cloud.CustomProvider{
			Config: cloud.NewProviderConfig(confMan, "../configs/default.json"),
		},
	}

	c.DownloadPricingData()
	k := c.GetKey(n.Labels, n)
	resN, err := c.NodePricing(k)
	if err != nil {
		t.Errorf("Error in NodePricing: %s", err.Error())
	} else {
		gotGPU := resN.GPU
		gotPrice := resN.Cost
		if gotGPU != wantGPU {
			t.Errorf("Wanted gpu count '%s' got gpu count '%s'", wantGPU, gotGPU)
		}
		if gotPrice != wantPrice {
			t.Errorf("Wanted price '%s' got price '%s'", wantPrice, gotPrice)
		}

	}

	k2 := c.GetKey(n2.Labels, n2)
	resN2, err := c.NodePricing(k2)
	if err != nil {
		t.Errorf("Error in NodePricing: %s", err.Error())
	} else {
		gotGPU := resN2.GPU
		gotPrice := resN2.Cost
		if gotGPU != wantGPU {
			t.Errorf("Wanted gpu count '%s' got gpu count '%s'", wantGPU, gotGPU)
		}
		if gotPrice != wantPrice2 {
			t.Errorf("Wanted price '%s' got price '%s'", wantPrice2, gotPrice)
		}

	}

}

func TestNodePriceFromCSV(t *testing.T) {
	providerIDWant := "providerid"
	nameWant := "gke-standard-cluster-1-pool-1-91dc432d-cg69"
	labelFooWant := "labelfoo"

	confMan := config.NewConfigFileManager(&config.ConfigFileManagerOpts{
		LocalConfigPath: "./",
	})

	n := &v1.Node{}
	n.Spec.ProviderID = providerIDWant
	n.Name = nameWant
	n.Labels = make(map[string]string)
	n.Labels["foo"] = labelFooWant

	wantPrice := "0.133700"

	c := &cloud.CSVProvider{
		CSVLocation: "../configs/pricing_schema.csv",
		CustomProvider: &cloud.CustomProvider{
			Config: cloud.NewProviderConfig(confMan, "../configs/default.json"),
		},
	}
	c.DownloadPricingData()
	k := c.GetKey(n.Labels, n)
	resN, err := c.NodePricing(k)
	if err != nil {
		t.Errorf("Error in NodePricing: %s", err.Error())
	} else {
		gotPrice := resN.Cost
		if gotPrice != wantPrice {
			t.Errorf("Wanted price '%s' got price '%s'", wantPrice, gotPrice)
		}
	}

	unknownN := &v1.Node{}
	unknownN.Spec.ProviderID = providerIDWant
	unknownN.Name = "unknownname"
	unknownN.Labels = make(map[string]string)
	unknownN.Labels["foo"] = labelFooWant
	unknownN.Labels["topology.kubernetes.io/region"] = "fakeregion"
	k2 := c.GetKey(unknownN.Labels, unknownN)
	resN2, _ := c.NodePricing(k2)
	if resN2 != nil {
		t.Errorf("CSV provider should return nil on missing node")
	}

	c2 := &cloud.CSVProvider{
		CSVLocation: "../configs/fake.csv",
		CustomProvider: &cloud.CustomProvider{
			Config: cloud.NewProviderConfig(confMan, "../configs/default.json"),
		},
	}
	k3 := c.GetKey(n.Labels, n)
	resN3, _ := c2.NodePricing(k3)
	if resN3 != nil {
		t.Errorf("CSV provider should return nil on missing csv")
	}
}

func TestNodePriceFromCSVWithRegion(t *testing.T) {
	providerIDWant := "gke-standard-cluster-1-pool-1-91dc432d-cg69"
	nameWant := "foo"
	labelFooWant := "labelfoo"

	confMan := config.NewConfigFileManager(&config.ConfigFileManagerOpts{
		LocalConfigPath: "./",
	})

	n := &v1.Node{}
	n.Spec.ProviderID = providerIDWant
	n.Name = nameWant
	n.Labels = make(map[string]string)
	n.Labels["foo"] = labelFooWant
	n.Labels[v1.LabelZoneRegion] = "regionone"
	wantPrice := "0.133700"

	n2 := &v1.Node{}
	n2.Spec.ProviderID = providerIDWant
	n2.Name = nameWant
	n2.Labels = make(map[string]string)
	n2.Labels["foo"] = labelFooWant
	n2.Labels[v1.LabelZoneRegion] = "regiontwo"
	wantPrice2 := "0.133800"

	n3 := &v1.Node{}
	n3.Spec.ProviderID = providerIDWant
	n3.Name = nameWant
	n3.Labels = make(map[string]string)
	n3.Labels["foo"] = labelFooWant
	n3.Labels[v1.LabelZoneRegion] = "fakeregion"
	wantPrice3 := "0.1339"

	c := &cloud.CSVProvider{
		CSVLocation: "../configs/pricing_schema_region.csv",
		CustomProvider: &cloud.CustomProvider{
			Config: cloud.NewProviderConfig(confMan, "../configs/default.json"),
		},
	}
	c.DownloadPricingData()
	k := c.GetKey(n.Labels, n)
	resN, err := c.NodePricing(k)
	if err != nil {
		t.Errorf("Error in NodePricing: %s", err.Error())
	} else {
		gotPrice := resN.Cost
		if gotPrice != wantPrice {
			t.Errorf("Wanted price '%s' got price '%s'", wantPrice, gotPrice)
		}
	}
	k2 := c.GetKey(n2.Labels, n2)
	resN2, err := c.NodePricing(k2)
	if err != nil {
		t.Errorf("Error in NodePricing: %s", err.Error())
	} else {
		gotPrice := resN2.Cost
		if gotPrice != wantPrice2 {
			t.Errorf("Wanted price '%s' got price '%s'", wantPrice2, gotPrice)
		}
	}
	k3 := c.GetKey(n3.Labels, n3)
	resN3, err := c.NodePricing(k3)
	if err != nil {
		t.Errorf("Error in NodePricing: %s", err.Error())
	} else {
		gotPrice := resN3.Cost
		if gotPrice != wantPrice3 {
			t.Errorf("Wanted price '%s' got price '%s'", wantPrice3, gotPrice)
		}
	}

	unknownN := &v1.Node{}
	unknownN.Spec.ProviderID = "fake providerID"
	unknownN.Name = "unknownname"
	unknownN.Labels = make(map[string]string)
	unknownN.Labels["topology.kubernetes.io/region"] = "fakeregion"
	unknownN.Labels["foo"] = labelFooWant
	k4 := c.GetKey(unknownN.Labels, unknownN)
	resN4, _ := c.NodePricing(k4)
	if resN4 != nil {
		t.Errorf("CSV provider should return nil on missing node, instead returned %+v", resN4)
	}

	c2 := &cloud.CSVProvider{
		CSVLocation: "../configs/fake.csv",
		CustomProvider: &cloud.CustomProvider{
			Config: cloud.NewProviderConfig(confMan, "../configs/default.json"),
		},
	}
	k5 := c.GetKey(n.Labels, n)
	resN5, _ := c2.NodePricing(k5)
	if resN5 != nil {
		t.Errorf("CSV provider should return nil on missing csv")
	}
}

type FakeCache struct {
	nodes []*v1.Node
	clustercache.ClusterCache
}

func (f FakeCache) GetAllNodes() []*v1.Node {
	return f.nodes
}

func (f FakeCache) GetAllDaemonSets() []*appsv1.DaemonSet {
	return nil
}

func NewFakeNodeCache(nodes []*v1.Node) FakeCache {
	return FakeCache{
		nodes: nodes,
	}
}

type FakeClusterMap struct {
	clusters.ClusterMap
}

func TestNodePriceFromCSVWithBadConfig(t *testing.T) {
	t.Setenv("CONFIG_PATH", "../config")
	confMan := config.NewConfigFileManager(&config.ConfigFileManagerOpts{
		LocalConfigPath: "./",
	})

	c := &cloud.CSVProvider{
		CSVLocation: "../configs/pricing_schema_case.csv",
		CustomProvider: &cloud.CustomProvider{
			Config: cloud.NewProviderConfig(confMan, "invalid.json"),
		},
	}
	c.DownloadPricingData()

	n := &v1.Node{}
	n.Spec.ProviderID = "fake"
	n.Name = "nameWant"
	n.Labels = make(map[string]string)
	n.Labels["foo"] = "labelFooWant"
	n.Labels[v1.LabelZoneRegion] = "regionone"

	fc := NewFakeNodeCache([]*v1.Node{n})
	fm := FakeClusterMap{}
	d, _ := time.ParseDuration("1m")

	model := costmodel.NewCostModel(nil, nil, fc, fm, d)

	_, err := model.GetNodeCost(c)
	if err != nil {
		t.Errorf("Error in node pricing: %s", err)
	}
}

func TestSourceMatchesFromCSV(t *testing.T) {
	t.Setenv("CONFIG_PATH", "../configs")

	confMan := config.NewConfigFileManager(&config.ConfigFileManagerOpts{
		LocalConfigPath: "./",
	})

	c := &cloud.CSVProvider{
		CSVLocation: "../configs/pricing_schema_case.csv",
		CustomProvider: &cloud.CustomProvider{
			Config: cloud.NewProviderConfig(confMan, "/default.json"),
		},
	}
	c.DownloadPricingData()

	n := &v1.Node{}
	n.Spec.ProviderID = "fake"
	n.Name = "nameWant"
	n.Labels = make(map[string]string)
	n.Labels["foo"] = "labelFooWant"
	n.Labels[v1.LabelZoneRegion] = "regionone"

	n2 := &v1.Node{}
	n2.Spec.ProviderID = "azure:///subscriptions/123a7sd-asd-1234-578a9-123abcdef/resourceGroups/case_12_STaGe_TeSt7/providers/Microsoft.Compute/virtualMachineScaleSets/vmss-agent-worker0-12stagetest7-ezggnore/virtualMachines/7"
	n2.Labels = make(map[string]string)
	n2.Labels[v1.LabelZoneRegion] = "eastus2"
	n2.Labels["foo"] = "labelFooWant"

	k := c.GetKey(n2.Labels, n2)
	resN, err := c.NodePricing(k)
	if err != nil {
		t.Errorf("Error in NodePricing: %s", err.Error())
	} else {
		wantPrice := "0.13370357"
		gotPrice := resN.Cost
		if gotPrice != wantPrice {
			t.Errorf("Wanted price '%s' got price '%s'", wantPrice, gotPrice)
		}
	}

	n3 := &v1.Node{}
	n3.Spec.ProviderID = "fake"
	n3.Name = "nameWant"
	n3.Labels = make(map[string]string)
	n.Labels[v1.LabelZoneRegion] = "eastus2"
	n.Labels[v1.LabelInstanceType] = "Standard_F32s_v2"

	fc := NewFakeNodeCache([]*v1.Node{n, n2, n3})
	fm := FakeClusterMap{}
	d, _ := time.ParseDuration("1m")

	model := costmodel.NewCostModel(nil, nil, fc, fm, d)

	_, err = model.GetNodeCost(c)
	if err != nil {
		t.Errorf("Error in node pricing: %s", err)
	}
	p, err := model.GetPricingSourceCounts()
	if err != nil {
		t.Errorf("Error in pricing source counts: %s", err)
	} else if p.TotalNodes != 3 {
		t.Errorf("Wanted 3 nodes got %d", p.TotalNodes)
	}
	if p.PricingTypeCounts[""] != 1 {
		t.Errorf("Wanted 1 default match got %d: %+v", p.PricingTypeCounts[""], p.PricingTypeCounts)
	}
	if p.PricingTypeCounts["csvExact"] != 1 {
		t.Errorf("Wanted 1 exact match got %d: %+v", p.PricingTypeCounts["csvExact"], p.PricingTypeCounts)
	}
	if p.PricingTypeCounts["csvClass"] != 1 {
		t.Errorf("Wanted 1 class match got %d: %+v", p.PricingTypeCounts["csvClass"], p.PricingTypeCounts)
	}

}

func TestNodePriceFromCSVWithCase(t *testing.T) {
	n := &v1.Node{}
	n.Spec.ProviderID = "azure:///subscriptions/123a7sd-asd-1234-578a9-123abcdef/resourceGroups/case_12_STaGe_TeSt7/providers/Microsoft.Compute/virtualMachineScaleSets/vmss-agent-worker0-12stagetest7-ezggnore/virtualMachines/7"
	n.Labels = make(map[string]string)
	n.Labels[v1.LabelZoneRegion] = "eastus2"
	wantPrice := "0.13370357"

	confMan := config.NewConfigFileManager(&config.ConfigFileManagerOpts{
		LocalConfigPath: "./",
	})

	c := &cloud.CSVProvider{
		CSVLocation: "../configs/pricing_schema_case.csv",
		CustomProvider: &cloud.CustomProvider{
			Config: cloud.NewProviderConfig(confMan, "../configs/default.json"),
		},
	}

	c.DownloadPricingData()
	k := c.GetKey(n.Labels, n)
	resN, err := c.NodePricing(k)
	if err != nil {
		t.Errorf("Error in NodePricing: %s", err.Error())
	} else {
		gotPrice := resN.Cost
		if gotPrice != wantPrice {
			t.Errorf("Wanted price '%s' got price '%s'", wantPrice, gotPrice)
		}
	}

}

func TestNodePriceFromCSVByClass(t *testing.T) {
	n := &v1.Node{}
	n.Spec.ProviderID = "fakeproviderid"
	n.Labels = make(map[string]string)
	n.Labels[v1.LabelZoneRegion] = "eastus2"
	n.Labels[v1.LabelInstanceType] = "Standard_F32s_v2"
	wantpricefloat := 0.13370357
	wantPrice := fmt.Sprintf("%f", (math.Round(wantpricefloat*1000000) / 1000000))

	confMan := config.NewConfigFileManager(&config.ConfigFileManagerOpts{
		LocalConfigPath: "./",
	})

	c := &cloud.CSVProvider{
		CSVLocation: "../configs/pricing_schema_case.csv",
		CustomProvider: &cloud.CustomProvider{
			Config: cloud.NewProviderConfig(confMan, "../configs/default.json"),
		},
	}

	c.DownloadPricingData()

	k := c.GetKey(n.Labels, n)
	resN, err := c.NodePricing(k)
	if err != nil {
		t.Errorf("Error in NodePricing: %s", err.Error())
	} else {
		gotPrice := resN.Cost
		if gotPrice != wantPrice {
			t.Errorf("Wanted price '%s' got price '%s'", wantPrice, gotPrice)
		}
	}

	n2 := &v1.Node{}
	n2.Spec.ProviderID = "fakeproviderid"
	n2.Labels = make(map[string]string)
	n2.Labels[v1.LabelZoneRegion] = "fakeregion"
	n2.Labels[v1.LabelInstanceType] = "Standard_F32s_v2"
	k2 := c.GetKey(n2.Labels, n)

	c.DownloadPricingData()
	resN2, err := c.NodePricing(k2)

	if resN2 != nil {
		t.Errorf("CSV provider should return nil on missing node, instead returned %+v", resN2)
	}

}

var awsDefaultRegions = []string{
	"us-east-2",
	"us-east-1",
	"us-west-1",
	"us-west-2",
	"ap-east-1",
	"ap-south-1",
	"ap-northeast-3",
	"ap-northeast-2",
	"ap-southeast-1",
	"ap-southeast-2",
	"ap-northeast-1",
	"ap-southeast-3",
	"ca-central-1",
	"cn-north-1",
	"cn-northwest-1",
	"eu-central-1",
	"eu-west-1",
	"eu-west-2",
	"eu-west-3",
	"eu-north-1",
	"eu-south-1",
	"me-south-1",
	"sa-east-1",
	"af-south-1",
	"us-gov-east-1",
	"us-gov-west-1",
}

func TestAwsRegions_ErrorReadingConfigFile(t *testing.T) {
	configDir := writeModel(t, "aws.json", `BAD JSON`)
	configFileManager := config.NewConfigFileManager(&config.ConfigFileManagerOpts{LocalConfigPath: configDir})

	node := &v1.Node{}
	node.Spec.ProviderID = "aws"

	awsProvider, err := cloud.NewProvider(NewFakeNodeCache([]*v1.Node{node}), "fake", configFileManager)

	if err != nil {
		t.Fatalf("Failed to create provider: %v", err)
	}

	actualRegions := awsProvider.Regions()
	if !slices.Equal(actualRegions, awsDefaultRegions) {
		t.Errorf("Expected regions: %v; Actual regions: %v", awsDefaultRegions, actualRegions)
	}
}

func TestAwsRegions_RegionsNotSetInConfigFile(t *testing.T) {
	configDir := writeModel(t, "aws.json", `{}`)
	configFileManager := config.NewConfigFileManager(&config.ConfigFileManagerOpts{LocalConfigPath: configDir})

	node := &v1.Node{}
	node.Spec.ProviderID = "aws"

	awsProvider, err := cloud.NewProvider(NewFakeNodeCache([]*v1.Node{node}), "fake", configFileManager)

	if err != nil {
		t.Fatalf("Failed to create provider: %v", err)
	}

	actualRegions := awsProvider.Regions()
	if !slices.Equal(actualRegions, awsDefaultRegions) {
		t.Errorf("Expected regions: %v; Actual regions: %v", awsDefaultRegions, actualRegions)
	}
}

func TestAwsRegions_RegionsSetInConfigFile(t *testing.T) {
	configDir := writeModel(t, "aws.json", `{"regions": ["us-east-1", "totally-fake"]}`)
	configFileManager := config.NewConfigFileManager(&config.ConfigFileManagerOpts{LocalConfigPath: configDir})

	node := &v1.Node{}
	node.Spec.ProviderID = "aws"

	awsProvider, err := cloud.NewProvider(NewFakeNodeCache([]*v1.Node{node}), "fake", configFileManager)

	if err != nil {
		t.Fatalf("Failed to create provider: %v", err)
	}

	expectedRegions := []string{"us-east-1", "totally-fake"}
	actualRegions := awsProvider.Regions()
	if !slices.Equal(actualRegions, expectedRegions) {
		t.Errorf("Expected regions: %v; Actual regions: %v", expectedRegions, actualRegions)
	}
}

var azureDefaultRegions = []string{
	"eastus",
	"eastus2",
	"southcentralus",
	"westus2",
	"westus3",
	"australiaeast",
	"southeastasia",
	"northeurope",
	"swedencentral",
	"uksouth",
	"westeurope",
	"centralus",
	"northcentralus",
	"westus",
	"southafricanorth",
	"centralindia",
	"eastasia",
	"japaneast",
	"jioindiawest",
	"koreacentral",
	"canadacentral",
	"francecentral",
	"germanywestcentral",
	"norwayeast",
	"switzerlandnorth",
	"uaenorth",
	"brazilsouth",
	"centralusstage",
	"eastusstage",
	"eastus2stage",
	"northcentralusstage",
	"southcentralusstage",
	"westusstage",
	"westus2stage",
	"asia",
	"asiapacific",
	"australia",
	"brazil",
	"canada",
	"europe",
	"france",
	"germany",
	"global",
	"india",
	"japan",
	"korea",
	"norway",
	"southafrica",
	"switzerland",
	"uae",
	"uk",
	"unitedstates",
	"eastasiastage",
	"southeastasiastage",
	"centraluseuap",
	"eastus2euap",
	"westcentralus",
	"southafricawest",
	"australiacentral",
	"australiacentral2",
	"australiasoutheast",
	"japanwest",
	"jioindiacentral",
	"koreasouth",
	"southindia",
	"westindia",
	"canadaeast",
	"francesouth",
	"germanynorth",
	"norwaywest",
	"switzerlandwest",
	"ukwest",
	"uaecentral",
	"brazilsoutheast",
	"usgovarizona",
	"usgoviowa",
	"usgovvirginia",
	"usgovtexas",
}

func TestAzureRegions_ErrorReadingConfigFile(t *testing.T) {
	configDir := writeModel(t, "azure.json", `BAD JSON`)
	configFileManager := config.NewConfigFileManager(&config.ConfigFileManagerOpts{LocalConfigPath: configDir})

	node := &v1.Node{}
	node.Spec.ProviderID = "azure"

	azureProvider, err := cloud.NewProvider(NewFakeNodeCache([]*v1.Node{node}), "fake", configFileManager)

	if err != nil {
		t.Fatalf("Failed to create provider: %v", err)
	}

	actualRegions := azureProvider.Regions()
	if !slices.Equal(actualRegions, azureDefaultRegions) {
		t.Errorf("Expected regions: %v; Actual regions: %v", azureDefaultRegions, actualRegions)
	}
}

func TestAzureRegions_RegionsNotSetInConfigFile(t *testing.T) {
	configDir := writeModel(t, "azure.json", `{}`)
	configFileManager := config.NewConfigFileManager(&config.ConfigFileManagerOpts{LocalConfigPath: configDir})

	node := &v1.Node{}
	node.Spec.ProviderID = "azure"

	azureProvider, err := cloud.NewProvider(NewFakeNodeCache([]*v1.Node{node}), "fake", configFileManager)

	if err != nil {
		t.Fatalf("Failed to create provider: %v", err)
	}

	actualRegions := azureProvider.Regions()
	if !slices.Equal(actualRegions, azureDefaultRegions) {
		t.Errorf("Expected regions: %v; Actual regions: %v", azureDefaultRegions, actualRegions)
	}
}

func TestAzureRegions_RegionsSetInConfigFile(t *testing.T) {
	configDir := writeModel(t, "azure.json", `{"regions": ["uksouth", "totallyfake"]}`)
	configFileManager := config.NewConfigFileManager(&config.ConfigFileManagerOpts{LocalConfigPath: configDir})

	node := &v1.Node{}
	node.Spec.ProviderID = "azure"

	azureProvider, err := cloud.NewProvider(NewFakeNodeCache([]*v1.Node{node}), "fake", configFileManager)

	if err != nil {
		t.Fatalf("Failed to create provider: %v", err)
	}

	expectedRegions := []string{"uksouth", "totallyfake"}
	actualRegions := azureProvider.Regions()
	if !slices.Equal(actualRegions, expectedRegions) {
		t.Errorf("Expected regions: %v; Actual regions: %v", expectedRegions, actualRegions)
	}
}

var gcpDefaultRegions = []string{
	"asia-east1",
	"asia-east2",
	"asia-northeast1",
	"asia-northeast2",
	"asia-northeast3",
	"asia-south1",
	"asia-south2",
	"asia-southeast1",
	"asia-southeast2",
	"australia-southeast1",
	"australia-southeast2",
	"europe-central2",
	"europe-north1",
	"europe-west1",
	"europe-west2",
	"europe-west3",
	"europe-west4",
	"europe-west6",
	"northamerica-northeast1",
	"northamerica-northeast2",
	"southamerica-east1",
	"us-central1",
	"us-east1",
	"us-east4",
	"us-west1",
	"us-west2",
	"us-west3",
	"us-west4",
}

func TestGcpRegions_ErrorReadingConfigFile(t *testing.T) {
	configDir := writeModel(t, "gcp.json", `BAD JSON`)
	configFileManager := config.NewConfigFileManager(&config.ConfigFileManagerOpts{LocalConfigPath: configDir})

	node := &v1.Node{}
	node.Spec.ProviderID = "gce"

	gcpProvider, err := cloud.NewProvider(NewFakeNodeCache([]*v1.Node{node}), "fake", configFileManager)

	if err != nil {
		t.Fatalf("Failed to create provider: %v", err)
	}

	actualRegions := gcpProvider.Regions()
	if !slices.Equal(actualRegions, gcpDefaultRegions) {
		t.Errorf("Expected regions: %v; Actual regions: %v", gcpDefaultRegions, actualRegions)
	}
}

func TestGcpRegions_RegionsNotSetInConfigFile(t *testing.T) {
	configDir := writeModel(t, "gcp.json", `{}`)
	configFileManager := config.NewConfigFileManager(&config.ConfigFileManagerOpts{LocalConfigPath: configDir})

	node := &v1.Node{}
	node.Spec.ProviderID = "gce"

	gcpProvider, err := cloud.NewProvider(NewFakeNodeCache([]*v1.Node{node}), "fake", configFileManager)

	if err != nil {
		t.Fatalf("Failed to create provider: %v", err)
	}

	actualRegions := gcpProvider.Regions()
	if !slices.Equal(actualRegions, gcpDefaultRegions) {
		t.Errorf("Expected regions: %v; Actual regions: %v", gcpDefaultRegions, actualRegions)
	}
}

func TestGcpRegions_RegionsSetInConfigFile(t *testing.T) {
	configDir := writeModel(t, "gcp.json", `{"regions": ["europe-west6", "totally-fake"]}`)
	configFileManager := config.NewConfigFileManager(&config.ConfigFileManagerOpts{LocalConfigPath: configDir})

	node := &v1.Node{}
	node.Spec.ProviderID = "gce"

	gcpProvider, err := cloud.NewProvider(NewFakeNodeCache([]*v1.Node{node}), "fake", configFileManager)

	if err != nil {
		t.Fatalf("Failed to create provider: %v", err)
	}

	expectedRegions := []string{"europe-west6", "totally-fake"}
	actualRegions := gcpProvider.Regions()
	if !slices.Equal(actualRegions, expectedRegions) {
		t.Errorf("Expected regions: %v; Actual regions: %v", expectedRegions, actualRegions)
	}
}

var scalewayDefaultRegions = []string{
	"fr-par-1",
	"fr-par-2",
	"fr-par-3",
	"nl-ams-1",
	"nl-ams-2",
	"pl-waw-1",
}

func TestScalewayRegions_ErrorReadingConfigFile(t *testing.T) {
	configDir := writeModel(t, "scaleway.json", `BAD JSON`)
	configFileManager := config.NewConfigFileManager(&config.ConfigFileManagerOpts{LocalConfigPath: configDir})

	node := &v1.Node{}
	node.Spec.ProviderID = "scaleway"

	scalewayProvider, err := cloud.NewProvider(NewFakeNodeCache([]*v1.Node{node}), "fake", configFileManager)

	if err != nil {
		t.Fatalf("Failed to create provider: %v", err)
	}

	actualRegions := scalewayProvider.Regions()
	if !slices.Equal(actualRegions, scalewayDefaultRegions) {
		t.Errorf("Expected regions: %v; Actual regions: %v", scalewayDefaultRegions, actualRegions)
	}
}

func TestScalewayRegions_RegionsNotSetInConfigFile(t *testing.T) {
	configDir := writeModel(t, "scaleway.json", `{}`)
	configFileManager := config.NewConfigFileManager(&config.ConfigFileManagerOpts{LocalConfigPath: configDir})

	node := &v1.Node{}
	node.Spec.ProviderID = "scaleway"

	scalewayProvider, err := cloud.NewProvider(NewFakeNodeCache([]*v1.Node{node}), "fake", configFileManager)

	if err != nil {
		t.Fatalf("Failed to create provider: %v", err)
	}

	actualRegions := scalewayProvider.Regions()
	if !slices.Equal(actualRegions, scalewayDefaultRegions) {
		t.Errorf("Expected regions: %v; Actual regions: %v", scalewayDefaultRegions, actualRegions)
	}
}

func TestScalewayRegions_RegionsSetInConfigFile(t *testing.T) {
	configDir := writeModel(t, "scaleway.json", `{"regions": ["fr-par-3", "totally-fake"]}`)
	configFileManager := config.NewConfigFileManager(&config.ConfigFileManagerOpts{LocalConfigPath: configDir})

	node := &v1.Node{}
	node.Spec.ProviderID = "scaleway"

	scalewayProvider, err := cloud.NewProvider(NewFakeNodeCache([]*v1.Node{node}), "fake", configFileManager)

	if err != nil {
		t.Fatalf("Failed to create provider: %v", err)
	}

	expectedRegions := []string{"fr-par-3", "totally-fake"}
	actualRegions := scalewayProvider.Regions()
	if !slices.Equal(actualRegions, expectedRegions) {
		t.Errorf("Expected regions: %v; Actual regions: %v", expectedRegions, actualRegions)
	}
}

func writeModel(t *testing.T, filename string, json string) (configDir string) {
	configDir = t.TempDir()
	modelsDir := filepath.Join(configDir, "models")
	configFile := filepath.Join(modelsDir, filename)

	var err error

	err = os.Mkdir(modelsDir, 0o775)
	if err != nil {
		t.Fatalf("Failed to create directory %s: %v", modelsDir, err)
	}

	err = os.WriteFile(configFile, []byte(json), 0o664)

	if err != nil {
		t.Fatalf("Failed to write file %s: %v", configFile, err)
	}

	return
}
