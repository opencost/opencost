package kubecost

import (
	"testing"
	"time"

	"github.com/opencost/opencost/pkg/util/timeutil"
)

var cciProperties1 = CloudCostItemProperties{
	ProviderID:  "providerid1",
	Provider:    "provider1",
	WorkGroupID: "workgroup1",
	BillingID:   "billing1",
	Service:     "service1",
	Category:    "category1",
	Labels: map[string]string{
		"label1": "value1",
		"label2": "value2",
	},
}

// TestCloudCostItem_LoadCloudCostItem checks that loaded CloudCostItems end up in the correct set in the
// correct proportions
func TestCloudCostItem_LoadCloudCostItem(t *testing.T) {
	// create values for 3 day Range tests
	end := RoundBack(time.Now().UTC(), timeutil.Day)
	start := end.Add(-3 * timeutil.Day)
	dayWindows, _ := GetWindows(start, end, timeutil.Day)
	emtpyCCISR, _ := NewCloudCostItemSetRange(start, end, timeutil.Day, "integration")
	testCases := map[string]struct {
		cci      []*CloudCostItem
		ccisr    *CloudCostItemSetRange
		expected []*CloudCostItemSet
	}{
		"Load Single Day On Grid": {
			cci: []*CloudCostItem{
				{
					Properties:   cciProperties1,
					Window:       dayWindows[0],
					IsKubernetes: true,
					Cost:         100,
					NetCost:      80,
				},
			},
			ccisr: emtpyCCISR.Clone(),
			expected: []*CloudCostItemSet{
				{
					Integration: "integration",
					Window:      dayWindows[0],
					CloudCostItems: map[string]*CloudCostItem{
						cciProperties1.Key(): {
							Properties:   cciProperties1,
							Window:       dayWindows[0],
							IsKubernetes: true,
							Cost:         100,
							NetCost:      80,
						},
					},
				},
				{
					Integration:    "integration",
					Window:         dayWindows[1],
					CloudCostItems: map[string]*CloudCostItem{},
				},
				{
					Integration:    "integration",
					Window:         dayWindows[2],
					CloudCostItems: map[string]*CloudCostItem{},
				},
			},
		},
		"Load Single Day Off Grid": {
			cci: []*CloudCostItem{
				{
					Properties:   cciProperties1,
					Window:       NewClosedWindow(start.Add(12*time.Hour), start.Add(36*time.Hour)),
					IsKubernetes: true,
					Cost:         100,
					NetCost:      80,
				},
			},
			ccisr: emtpyCCISR.Clone(),
			expected: []*CloudCostItemSet{
				{
					Integration: "integration",
					Window:      dayWindows[0],
					CloudCostItems: map[string]*CloudCostItem{
						cciProperties1.Key(): {
							Properties:   cciProperties1,
							Window:       NewClosedWindow(start.Add(12*time.Hour), start.Add(24*time.Hour)),
							IsKubernetes: true,
							Cost:         50,
							NetCost:      40,
						},
					},
				},
				{
					Integration: "integration",
					Window:      dayWindows[1],
					CloudCostItems: map[string]*CloudCostItem{
						cciProperties1.Key(): {
							Properties:   cciProperties1,
							Window:       NewClosedWindow(start.Add(24*time.Hour), start.Add(36*time.Hour)),
							IsKubernetes: true,
							Cost:         50,
							NetCost:      40,
						},
					},
				},
				{
					Integration:    "integration",
					Window:         dayWindows[2],
					CloudCostItems: map[string]*CloudCostItem{},
				},
			},
		},
		"Load Single Day Off Grid Before Range Window": {
			cci: []*CloudCostItem{
				{
					Properties:   cciProperties1,
					Window:       NewClosedWindow(start.Add(-12*time.Hour), start.Add(12*time.Hour)),
					IsKubernetes: true,
					Cost:         100,
					NetCost:      80,
				},
			},
			ccisr: emtpyCCISR.Clone(),
			expected: []*CloudCostItemSet{
				{
					Integration: "integration",
					Window:      dayWindows[0],
					CloudCostItems: map[string]*CloudCostItem{
						cciProperties1.Key(): {
							Properties:   cciProperties1,
							Window:       NewClosedWindow(start, start.Add(12*time.Hour)),
							IsKubernetes: true,
							Cost:         50,
							NetCost:      40,
						},
					},
				},
				{
					Integration:    "integration",
					Window:         dayWindows[1],
					CloudCostItems: map[string]*CloudCostItem{},
				},
				{
					Integration:    "integration",
					Window:         dayWindows[2],
					CloudCostItems: map[string]*CloudCostItem{},
				},
			},
		},
		"Load Single Day Off Grid After Range Window": {
			cci: []*CloudCostItem{
				{
					Properties:   cciProperties1,
					Window:       NewClosedWindow(end.Add(-12*time.Hour), end.Add(12*time.Hour)),
					IsKubernetes: true,
					Cost:         100,
					NetCost:      80,
				},
			},
			ccisr: emtpyCCISR.Clone(),
			expected: []*CloudCostItemSet{
				{
					Integration:    "integration",
					Window:         dayWindows[0],
					CloudCostItems: map[string]*CloudCostItem{},
				},
				{
					Integration:    "integration",
					Window:         dayWindows[1],
					CloudCostItems: map[string]*CloudCostItem{},
				},
				{
					Integration: "integration",
					Window:      dayWindows[2],
					CloudCostItems: map[string]*CloudCostItem{
						cciProperties1.Key(): {
							Properties:   cciProperties1,
							Window:       NewClosedWindow(end.Add(-12*time.Hour), end),
							IsKubernetes: true,
							Cost:         50,
							NetCost:      40,
						},
					},
				},
			},
		},
		"Single Day Kubecost Percent": {
			cci: []*CloudCostItem{
				{
					Properties:   cciProperties1,
					Window:       dayWindows[1],
					IsKubernetes: true,
					Cost:         75,
					NetCost:      60,
				},
				{
					Properties:   cciProperties1,
					Window:       dayWindows[1],
					IsKubernetes: true,
					Cost:         25,
					NetCost:      20,
				},
			},
			ccisr: emtpyCCISR.Clone(),
			expected: []*CloudCostItemSet{
				{
					Integration:    "integration",
					Window:         dayWindows[0],
					CloudCostItems: map[string]*CloudCostItem{},
				},
				{
					Integration: "integration",
					Window:      dayWindows[1],
					CloudCostItems: map[string]*CloudCostItem{
						cciProperties1.Key(): {
							Properties:   cciProperties1,
							Window:       dayWindows[1],
							IsKubernetes: true,
							Cost:         100,
							NetCost:      80,
						},
					},
				},
				{
					Integration:    "integration",
					Window:         dayWindows[2],
					CloudCostItems: map[string]*CloudCostItem{},
				},
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			// load Cloud Cost Items
			for _, cci := range tc.cci {
				tc.ccisr.LoadCloudCostItem(cci)
			}

			if len(tc.ccisr.CloudCostItemSets) != len(tc.expected) {
				t.Errorf("the CloudCostItemSetRanges did not have the expected length")
			}

			for i, ccis := range tc.ccisr.CloudCostItemSets {
				if !ccis.Equal(tc.expected[i]) {
					t.Errorf("CloudCostItemSet at index: %d did not match expected", i)
				}
			}
		})
	}

}

func TestGetAWSClusterFromCCI(t *testing.T) {
	awsCCIWithLabeleksClusterName, eksClusterName := GenerateAWSMockCCIAndPID(1, 1, AWSMatchLabel1, ComputeCategory)
	awsCCIWithLabeleksCtlClusterName, eksCtlClusterName := GenerateAWSMockCCIAndPID(2, 2, AWSMatchLabel2, ComputeCategory)
	awsCCIWithLabelWithRandomLabel, _ := GenerateAWSMockCCIAndPID(1, 1, "randomLabel", ComputeCategory)
	awsCCINetworkCategory, _ := GenerateAWSMockCCIAndPID(1, 1, AWSMatchLabel1, NetworkCategory)
	alibabaCCI, _ := GenerateAlibabaMockCCIAndPID(4, 4, AlibabaMatchLabel1, ComputeCategory)
	testCases := map[string]struct {
		testcci  *CloudCostItem
		expected string
	}{
		"cluster in label eks_cluster_name": {
			testcci:  awsCCIWithLabeleksClusterName,
			expected: eksClusterName,
		},
		"cluster in label alpha_eksctl_io_cluster_name": {
			testcci:  awsCCIWithLabeleksCtlClusterName,
			expected: eksCtlClusterName,
		},
		"cluster name in random label either not eks_cluster_name or eks_cluster_name": {
			testcci:  awsCCIWithLabelWithRandomLabel,
			expected: "",
		},
		"Not a AWS provider": {
			testcci:  alibabaCCI,
			expected: "",
		},
		"Not a compute resource": {
			testcci:  awsCCINetworkCategory,
			expected: "",
		},
	}
	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			actual := testCase.testcci.GetAWSCluster()
			if actual != testCase.expected {
				t.Errorf("incorrect result: Actual: '%s', Expected: '%s", actual, testCase.expected)
			}
		})
	}
}

func TestGetAzureClusterFromCCI(t *testing.T) {
	testCases := map[string]struct {
		testcci  *CloudCostItem
		expected string
	}{
		"cluster in ProviderID complete": {
			testcci: &CloudCostItem{
				IsKubernetes: true,
				Window:       Window{},
				Properties: CloudCostItemProperties{
					Labels: map[string]string{
						"randomLabel": "value1",
					},
					Provider:   AzureProvider,
					Category:   ComputeCategory,
					ProviderID: "azure:///subscriptions/0bd50fdf-c923-4e1e-850c-196dd3dcc5d3/resourceGroups/mc_dev_dev-1_eastus/providers/Microsoft.Compute/virtualMachineScaleSets/aks-devsysz1-24570986-vmss/virtualMachines/0",
				},
			},
			expected: "mc_dev_dev-1_eastus",
		},
		"cluster in ProviderID complete but missing some values": {
			testcci: &CloudCostItem{
				IsKubernetes: true,
				Window:       Window{},
				Properties: CloudCostItemProperties{
					Labels: map[string]string{
						"randomLabel": "value1",
					},
					Provider:   AzureProvider,
					Category:   ComputeCategory,
					ProviderID: "azure:///subscriptions//resourceGroups/mc_dev_dev-1_eastus/providers/Microsoft.Compute/virtualMachineScaleSets/aks-devsysz1-XXXXX-vmss/virtualMachines/0",
				},
			},
			expected: "mc_dev_dev-1_eastus",
		},
		"Not having enough split content in providerID": {
			testcci: &CloudCostItem{
				IsKubernetes: true,
				Window:       Window{},
				Properties: CloudCostItemProperties{
					Labels: map[string]string{
						"randomLabel": "value1",
					},
					Provider:   AzureProvider,
					Category:   ComputeCategory,
					ProviderID: "test1",
				},
			},
			expected: "",
		},
		"Not a Azure provider": {
			testcci: &CloudCostItem{
				IsKubernetes: true,
				Window:       Window{},
				Properties: CloudCostItemProperties{
					Labels: map[string]string{
						"randomLabel": "value1",
					},
					Provider:   AWSProvider,
					Category:   ComputeCategory,
					ProviderID: "test1",
				},
			},
			expected: "",
		},
		"Not a compute resource": {
			testcci: &CloudCostItem{
				IsKubernetes: true,
				Window:       Window{},
				Properties: CloudCostItemProperties{
					Labels: map[string]string{
						"randomLabel": "value1",
					},
					Provider:   AzureProvider,
					Category:   StorageCategory,
					ProviderID: "pvc-xyz",
				},
			},
			expected: "",
		},
	}
	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			actual := testCase.testcci.GetAzureCluster()
			if actual != testCase.expected {
				t.Errorf("incorrect result: Actual: '%s', Expected: '%s", actual, testCase.expected)
			}
		})
	}
}

func TestGetAlibabaClusterFromCCI(t *testing.T) {
	alibabaCCIWithACKAliyunCom, clusterName1 := GenerateAlibabaMockCCIAndPID(4, 4, AlibabaMatchLabel1, ComputeCategory)
	awsCCI, _ := GenerateAWSMockCCIAndPID(1, 1, AWSMatchLabel1, ComputeCategory)
	alibabaCCINetworkCategory, clusterName1 := GenerateAlibabaMockCCIAndPID(4, 4, AlibabaMatchLabel1, NetworkCategory)
	testCases := map[string]struct {
		testcci  *CloudCostItem
		expected string
	}{
		"cluster in label ack.aliyun.com": {
			testcci:  alibabaCCIWithACKAliyunCom,
			expected: clusterName1,
		},
		"Not a Alibaba provider": {
			testcci:  awsCCI,
			expected: "",
		},
		"Not a compute resource": {
			testcci:  alibabaCCINetworkCategory,
			expected: "",
		},
	}
	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			actual := testCase.testcci.GetAlibabaCluster()
			if actual != testCase.expected {
				t.Errorf("incorrect result: Actual: '%s', Expected: '%s", actual, testCase.expected)
			}
		})
	}
}
