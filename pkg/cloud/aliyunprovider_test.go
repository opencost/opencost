package cloud

import (
	"fmt"
	"testing"

	"github.com/aliyun/alibaba-cloud-sdk-go/sdk"
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/auth/credentials"
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/auth/signers"
	"github.com/opencost/opencost/pkg/log"
	v1 "k8s.io/api/core/v1"
	resource "k8s.io/apimachinery/pkg/api/resource"
)

func TestCreateDescribePriceACSRequest(t *testing.T) {
	node := &SlimK8sNode{
		InstanceType:       "ecs.g6.large",
		RegionID:           "cn-hangzhou",
		PriceUnit:          "Hour",
		MemorySizeInKiB:    "16KiB",
		IsIoOptimized:      true,
		OSType:             "Linux",
		ProviderID:         "Ali-XXX-node-01",
		InstanceTypeFamily: "g6",
	}
	_, err := createDescribePriceACSRequest(node)
	if err != nil {
		t.Errorf("Error converting to Alibaba cloud request")
	}
}

func TestProcessDescribePriceAndCreateAliyunPricing(t *testing.T) {
	// Skipping this test case since it exposes secret but a good test case to verify when
	// supporting a new family of instances, steps to perform are
	// STEP 1: Comment the t.Skip() line and then replace XXX_KEY_ID with the aliyun key id of your account and XXX_SECRET_ID with aliyun secret of your account.
	// STEP 2: Once you verify describePrice is working and no change needed in processDescribePriceAndCreateAliyunPricing, you can go ahead and revert the step 1 changes.

	// This test case was use to test all general puprose instances

	t.Skip()

	client, err := sdk.NewClientWithAccessKey("cn-hangzhou", "XXX_KEY_ID", "XXX_SECRET_ID")
	if err != nil {
		t.Errorf("Error connecting to the Alibaba cloud")
	}
	aak := credentials.NewAccessKeyCredential("XXX_KEY_ID", "XXX_SECRET_ID")
	signer := signers.NewAccessKeySigner(aak)

	cases := []struct {
		name          string
		testNode      *SlimK8sNode
		expectedError error
	}{
		{
			name: "test Enhanced General Purpose Type g6e instance family",
			testNode: &SlimK8sNode{
				InstanceType:       "ecs.g6e.xlarge",
				RegionID:           "cn-hangzhou",
				PriceUnit:          "Hour",
				MemorySizeInKiB:    "16777216KiB",
				IsIoOptimized:      true,
				OSType:             "Linux",
				ProviderID:         "cn-hangzhou.i-test-01",
				InstanceTypeFamily: "g6e",
			},
			expectedError: nil,
		},
		{
			name: "test General Purpose Type g6 instance family",
			testNode: &SlimK8sNode{
				InstanceType:       "ecs.g6.3xlarge",
				RegionID:           "cn-hangzhou",
				PriceUnit:          "Hour",
				MemorySizeInKiB:    "50331648KiB",
				IsIoOptimized:      true,
				OSType:             "Linux",
				ProviderID:         "cn-hangzhou.i-test-02",
				InstanceTypeFamily: "g6",
			},
			expectedError: nil,
		},
		{
			name: "test General Purpose Type g5 instance family",
			testNode: &SlimK8sNode{
				InstanceType:       "ecs.g5.2xlarge",
				RegionID:           "cn-hangzhou",
				PriceUnit:          "Hour",
				MemorySizeInKiB:    "33554432KiB",
				IsIoOptimized:      true,
				OSType:             "Linux",
				ProviderID:         "cn-hangzhou.i-test-03",
				InstanceTypeFamily: "g5",
			},
			expectedError: nil,
		},
		{
			name: "test General Purpose Type sn2 instance family",
			testNode: &SlimK8sNode{
				InstanceType:       "ecs.sn2.large",
				RegionID:           "cn-hangzhou",
				PriceUnit:          "Hour",
				MemorySizeInKiB:    "16777216KiB",
				IsIoOptimized:      true,
				OSType:             "Linux",
				ProviderID:         "cn-hangzhou.i-test-04",
				InstanceTypeFamily: "sn2",
			},
			expectedError: nil,
		},
		{
			name: "test General Purpose Type with Enhanced Network Performance sn2ne instance family",
			testNode: &SlimK8sNode{
				InstanceType:       "ecs.sn2ne.2xlarge",
				RegionID:           "cn-hangzhou",
				PriceUnit:          "Hour",
				MemorySizeInKiB:    "33554432KiB",
				IsIoOptimized:      true,
				OSType:             "Linux",
				ProviderID:         "cn-hangzhou.i-test-05",
				InstanceTypeFamily: "sn2ne",
			},
			expectedError: nil,
		},
	}
	custom := &CustomPricing{}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pricingObj, err := processDescribePriceAndCreateAliyunPricing(client, c.testNode, signer, custom)
			if err != nil && c.expectedError == nil {
				t.Fatalf("Case name %s: got an error %s", c.name, err)
			}
			if pricingObj == nil {
				t.Fatalf("Case name %s: got a nil pricing object", c.name)
			}
			t.Logf("Pricing Information gathered for instanceType %s is %v", c.name, pricingObj.PricingTerms.PricingDetails.TradePrice)
		})
	}
}

func TestGetInstanceFamilyFromType(t *testing.T) {
	cases := []struct {
		name                   string
		instanceType           string
		expectedInstanceFamily string
	}{
		{
			name:                   "test if ecs.[instance-family].[different-type] work",
			instanceType:           "ecs.sn2ne.2xlarge",
			expectedInstanceFamily: "sn2ne",
		},
		{
			name:                   "test if random word gives you ALIYUN_UNKNOWN_INSTANCE_FAMILY_TYPE value ",
			instanceType:           "random.value",
			expectedInstanceFamily: ALIYUN_UNKNOWN_INSTANCE_FAMILY_TYPE,
		},
		{
			name:                   "test if random instance family gives you ALIYUN_NOT_SUPPORTED_INSTANCE_FAMILY_TYPE value ",
			instanceType:           "ecs.g7e.2xlarge",
			expectedInstanceFamily: ALIYUN_NOT_SUPPORTED_INSTANCE_FAMILY_TYPE,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			returnValue := getInstanceFamilyFromType(c.instanceType)
			if returnValue != c.expectedInstanceFamily {
				t.Fatalf("Case name %s: expected instance family of type %s but got %s", c.name, c.expectedInstanceFamily, returnValue)
			}
		})
	}
}

func TestDetermineKeyForPricing(t *testing.T) {
	type randomK8sStruct struct {
		name string
	}
	cases := []struct {
		name          string
		testVar       interface{}
		expectedKey   string
		expectedError error
	}{
		{
			name: "test when all RegionID, InstanceType, OSType & ALIYUN_OPTIMIZE_KEYWORD words are used to key",
			testVar: &SlimK8sNode{
				InstanceType:       "ecs.sn2.large",
				RegionID:           "cn-hangzhou",
				PriceUnit:          "Hour",
				MemorySizeInKiB:    "16777216KiB",
				IsIoOptimized:      true,
				OSType:             "linux",
				ProviderID:         "cn-hangzhou.i-test-04",
				InstanceTypeFamily: "sn2",
			},
			expectedKey:   "cn-hangzhou::ecs.sn2.large::linux::optimize",
			expectedError: nil,
		},
		{
			name: "test missing InstanceType to create key",
			testVar: &SlimK8sNode{
				RegionID:        "cn-hangzhou",
				PriceUnit:       "Hour",
				MemorySizeInKiB: "16777216KiB",
				IsIoOptimized:   true,
				OSType:          "linux",
				ProviderID:      "cn-hangzhou.i-test-04",
			},
			expectedKey:   "cn-hangzhou::linux::optimize",
			expectedError: nil,
		},
		{
			name: "test random k8s struct should return unsupported error",
			testVar: &randomK8sStruct{
				name: "test struct",
			},
			expectedKey:   "",
			expectedError: fmt.Errorf("unsupported ECS type for DescribePrice at this time"),
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			returnString, returnErr := determineKeyForPricing(c.testVar)
			if c.expectedError == nil && returnErr != nil {
				t.Fatalf("Case name %s: expected error was nil but recieved error %v", c.name, returnErr)
			}
			if returnString != c.expectedKey {
				t.Fatalf("Case name %s: determineKeyForPricing recieved %s but expected %s", c.name, returnString, c.expectedKey)
			}
		})
	}
}

func TestGenerateSlimK8sNodeFromV1Node(t *testing.T) {
	testv1Node := &v1.Node{}
	testv1Node.Labels = make(map[string]string)
	testv1Node.Labels["topology.kubernetes.io/region"] = "us-east-1"
	testv1Node.Labels["beta.kubernetes.io/os"] = "linux"
	testv1Node.Labels["node.kubernetes.io/instance-type"] = "ecs.sn2ne.2xlarge"
	testv1Node.Status.Capacity = v1.ResourceList{
		v1.ResourceMemory: *resource.NewQuantity(16, resource.BinarySI),
	}
	cases := []struct {
		name             string
		testNode         *v1.Node
		expectedSlimNode *SlimK8sNode
	}{
		{
			name:     "test a generic *v1.Node to *SlimK8sNode Conversion",
			testNode: testv1Node,
			expectedSlimNode: &SlimK8sNode{
				InstanceType:       "ecs.sn2ne.2xlarge",
				RegionID:           "us-east-1",
				PriceUnit:          "Hour",
				MemorySizeInKiB:    "16",
				IsIoOptimized:      true,
				OSType:             "linux",
				InstanceTypeFamily: "sn2ne",
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			returnSlimK8sNode := generateSlimK8sNodeFromV1Node(c.testNode)
			log.Infof("value is %v", returnSlimK8sNode)
			if returnSlimK8sNode.InstanceType != c.expectedSlimNode.InstanceType {
				t.Fatalf("unexpected conversion in function generateSlimK8sNodeFromV1Node expected InstanceType: %s , recieved Instance Type: %s", c.expectedSlimNode.InstanceType, returnSlimK8sNode.InstanceType)
			}
			if returnSlimK8sNode.RegionID != c.expectedSlimNode.RegionID {
				t.Fatalf("unexpected conversion in function generateSlimK8sNodeFromV1Node expected RegionID: %s , recieved RegionID Type: %s", c.expectedSlimNode.RegionID, returnSlimK8sNode.RegionID)
			}
			if returnSlimK8sNode.PriceUnit != c.expectedSlimNode.PriceUnit {
				t.Fatalf("unexpected conversion in function generateSlimK8sNodeFromV1Node expected PriceUnit: %s , recieved PriceUnit Type: %s", c.expectedSlimNode.PriceUnit, returnSlimK8sNode.PriceUnit)
			}
			if returnSlimK8sNode.MemorySizeInKiB != c.expectedSlimNode.MemorySizeInKiB {
				t.Fatalf("unexpected conversion in function generateSlimK8sNodeFromV1Node expected MemorySizeInKiB: %s , recieved MemorySizeInKiB Type: %s", c.expectedSlimNode.MemorySizeInKiB, returnSlimK8sNode.MemorySizeInKiB)
			}
			if returnSlimK8sNode.OSType != c.expectedSlimNode.OSType {
				t.Fatalf("unexpected conversion in function generateSlimK8sNodeFromV1Node expected OSType: %s , recieved OSType Type: %s", c.expectedSlimNode.OSType, returnSlimK8sNode.OSType)
			}
			if returnSlimK8sNode.InstanceTypeFamily != c.expectedSlimNode.InstanceTypeFamily {
				t.Fatalf("unexpected conversion in function generateSlimK8sNodeFromV1Node expected InstanceTypeFamily: %s , recieved InstanceTypeFamily Type: %s", c.expectedSlimNode.InstanceTypeFamily, returnSlimK8sNode.InstanceTypeFamily)
			}
		})
	}
}
