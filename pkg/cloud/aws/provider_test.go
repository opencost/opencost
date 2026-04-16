package aws

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/config"
	v1 "k8s.io/api/core/v1"
)

func Test_awsKey_getUsageType(t *testing.T) {
	type fields struct {
		Labels     map[string]string
		ProviderID string
	}
	type args struct {
		labels map[string]string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		{
			// test with no labels should return false
			name: "Label does not have the capacityType label associated with it",
			args: args{
				labels: map[string]string{},
			},
			want: "",
		},
		{
			name: "EKS label with a capacityType set to empty string should return empty string",
			args: args{
				labels: map[string]string{
					EKSCapacityTypeLabel: "",
				},
			},
			want: "",
		},
		{
			name: "EKS label with capacityType set to a random value should return empty string",
			args: args{
				labels: map[string]string{
					EKSCapacityTypeLabel: "TEST_ME",
				},
			},
			want: "",
		},
		{
			name: "EKS label with capacityType set to spot should return spot",
			args: args{
				labels: map[string]string{
					EKSCapacityTypeLabel: EKSCapacitySpotTypeValue,
				},
			},
			want: PreemptibleType,
		},
		{
			name: "Karpenter label with a capacityType set to empty string should return empty string",
			args: args{
				labels: map[string]string{
					models.KarpenterCapacityTypeLabel: "",
				},
			},
			want: "",
		},
		{
			name: "Karpenter label with capacityType set to a random value should return empty string",
			args: args{
				labels: map[string]string{
					models.KarpenterCapacityTypeLabel: "TEST_ME",
				},
			},
			want: "",
		},
		{
			name: "Karpenter label with capacityType set to spot should return spot",
			args: args{
				labels: map[string]string{
					models.KarpenterCapacityTypeLabel: models.KarpenterCapacitySpotTypeValue,
				},
			},
			want: PreemptibleType,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := &awsKey{
				Labels:     tt.fields.Labels,
				ProviderID: tt.fields.ProviderID,
			}
			if got := k.getUsageType(tt.args.labels); got != tt.want {
				t.Errorf("getUsageType() = %v, want %v", got, tt.want)
			}
		})
	}
}

// Test_PricingData_Regression
//
// Objective: To test the pricing data download and validate the schema is still
// as expected
//
// These tests may take a long time to complete. It is downloading AWS Pricing
// data files (~500MB) for each region.
func Test_PricingData_Regression(t *testing.T) {
	if os.Getenv("INTEGRATION") == "" {
		t.Skip("skipping integration tests, set environment variable INTEGRATION")
	}

	awsRegions := []string{"us-east-1", "eu-west-1"}

	// Check pricing data produced for each region
	for _, region := range awsRegions {

		awsTest := AWS{}
		res, _, err := awsTest.getRegionPricing([]*clustercache.Node{
			{
				Labels: map[string]string{"topology.kubernetes.io/region": region},
			}})
		if err != nil {
			t.Errorf("Failed to download pricing data for region %s: %v", region, err)
		}

		// Unmarshal pricing data into AWSPricing
		var pricingData AWSPricing
		body, err := io.ReadAll(res.Body)
		if err != nil {
			t.Errorf("Failed to read pricing data for region %s: %v", region, err)
		}
		err = json.Unmarshal(body, &pricingData)
		if err != nil {
			t.Errorf("Failed to unmarshal pricing data for region %s: %v", region, err)
		}

		// ASSERTION. We only anticipate "OnDemand" or "CapacityBlock" in the
		// pricing data.
		//
		// Failing this test does not necessarily mean we have regressed. Just
		// that we need to revisit this code to ensure OnDemand pricing is still
		// functioning as expected.
		for _, product := range pricingData.Products {
			if product.Attributes.MarketOption != "OnDemand" && product.Attributes.MarketOption != "CapacityBlock" && product.Attributes.MarketOption != "" {
				t.Errorf("Invalid marketOption for product %s: %s", product.Sku, product.Attributes.MarketOption)
			}
		}
	}
}

// Test_populate_pricing
//
// Objective: To test core pricing population logic for AWS
//
// Case 0: US endpoints
// Take a portion of json returned from ondemand terms in us endpoints load the
// request into the http response and give it to the function inspect the
// resulting aws object after the function returns and validate fields
//
// Case 1: Ensure marketOption=OnDemand
// AWS introduced the field marketOption. We need to further filter for
// marketOption=OnDemand to ensure we are not getting pricing from a line item
// such as marketOption=CapacityBlock
//
// Case 2: Chinese endpoints
// Same as above US test case, except using CN PV offer codes. Validate
// populated fields in AWS object
func Test_populate_pricing(t *testing.T) {
	awsTest := AWS{
		ValidPricingKeys: map[string]bool{},
		ClusterRegion:    "us-east-2",
	}
	inputkeys := map[string]bool{
		"us-east-2,m5.large,linux": true,
	}

	fixture, err := os.Open("testdata/pricing-us-east-2.json")
	if err != nil {
		t.Fatalf("failed to load pricing fixture: %s", err)
	}

	testResponse := http.Response{
		Body: io.NopCloser(fixture),
		Request: &http.Request{
			URL: &url.URL{
				Scheme: "https",
				Host:   "test-aws-http-endpoint:443",
			},
		},
	}

	awsTest.populatePricing(&testResponse, inputkeys)

	expectedProdTermsDisk := &AWSProductTerms{
		Sku:     "M6UGCCQ3CDJQAA37",
		Memory:  "",
		Storage: "",
		VCpu:    "",
		GPU:     "",
		OnDemand: &AWSOfferTerm{
			Sku:           "M6UGCCQ3CDJQAA37",
			OfferTermCode: "JRTCKXETXF",
			PriceDimensions: map[string]*AWSRateCode{
				"M6UGCCQ3CDJQAA37.JRTCKXETXF.6YS6EN2CT7": {
					Unit: "GB-Mo",
					PricePerUnit: AWSCurrencyCode{
						USD: "0.0800000000",
						CNY: "",
					},
				},
			},
		},
		PV: &models.PV{
			Cost:       "0.00010958904109589041",
			CostPerIO:  "",
			Class:      "gp3",
			Size:       "",
			Region:     "us-east-2",
			ProviderID: "",
		},
	}

	expectedProdTermsInstanceOndemand := &AWSProductTerms{
		Sku:     "8D49XP354UEYTHGM",
		Memory:  "8 GiB",
		Storage: "EBS only",
		VCpu:    "2",
		GPU:     "",
		OnDemand: &AWSOfferTerm{
			Sku:           "8D49XP354UEYTHGM",
			OfferTermCode: "MZU6U2429S",
			PriceDimensions: map[string]*AWSRateCode{
				"8D49XP354UEYTHGM.MZU6U2429S.2TG2D8R56U": {
					Unit: "Quantity",
					PricePerUnit: AWSCurrencyCode{
						USD: "1161",
						CNY: "",
					},
				},
			},
		},
	}

	expectedProdTermsInstanceSpot := &AWSProductTerms{
		Sku:     "8D49XP354UEYTHGM",
		Memory:  "8 GiB",
		Storage: "EBS only",
		VCpu:    "2",
		GPU:     "",
		OnDemand: &AWSOfferTerm{
			Sku:           "8D49XP354UEYTHGM",
			OfferTermCode: "MZU6U2429S",
			PriceDimensions: map[string]*AWSRateCode{
				"8D49XP354UEYTHGM.MZU6U2429S.2TG2D8R56U": {
					Unit: "Quantity",
					PricePerUnit: AWSCurrencyCode{
						USD: "1161",
						CNY: "",
					},
				},
			},
		},
	}

	expectedProdTermsLoadbalancer := &AWSProductTerms{
		Sku: "Y9RYMSE644KDSV4S",
		OnDemand: &AWSOfferTerm{
			Sku:           "Y9RYMSE644KDSV4S",
			OfferTermCode: "JRTCKXETXF",
			PriceDimensions: map[string]*AWSRateCode{
				"Y9RYMSE644KDSV4S.JRTCKXETXF.6YS6EN2CT7": {
					Unit: "Hrs",
					PricePerUnit: AWSCurrencyCode{
						USD: "0.0225000000",
						CNY: "",
					},
				},
			},
		},
		LoadBalancer: &models.LoadBalancer{
			Cost: 0.0225,
		},
	}

	expectedPricing := map[string]*AWSProductTerms{
		"us-east-2,EBS:VolumeUsage.gp3":             expectedProdTermsDisk,
		"us-east-2,EBS:VolumeUsage.gp3,preemptible": expectedProdTermsDisk,
		"us-east-2,m5.large,linux":                  expectedProdTermsInstanceOndemand,
		"us-east-2,m5.large,linux,preemptible":      expectedProdTermsInstanceSpot,
		"us-east-2,LoadBalancerUsage":               expectedProdTermsLoadbalancer,
	}

	if !reflect.DeepEqual(expectedPricing, awsTest.Pricing) {
		t.Fatalf("expected parsed pricing did not match actual parsed result (us-east-2)")
	}

	lbPricing, _ := awsTest.LoadBalancerPricing()
	if lbPricing.Cost != 0.0225 {
		t.Fatalf("expected loadbalancer pricing of 0.0225 but got %f (us-east-2)", lbPricing.Cost)
	}

	// Case 1 - Only accept `"marketoption":"OnDemand"`
	inputkeysCase1 := map[string]bool{
		"us-east-1,p4d.24xlarge,linux": true,
	}

	fixture, err = os.Open("testdata/pricing-us-east-1.json")
	if err != nil {
		t.Fatalf("failed to load pricing fixture: %s", err)
	}

	testResponseCase1 := http.Response{
		Body: io.NopCloser(fixture),
		Request: &http.Request{
			URL: &url.URL{
				Scheme: "https",
				Host:   "test-aws-http-endpoint:443",
			},
		},
	}

	awsTest.populatePricing(&testResponseCase1, inputkeysCase1)

	expectedProdTermsInstanceOndemandCase1 := &AWSProductTerms{
		Sku:     "H7NGEAC6UEHNTKSJ",
		Memory:  "1152 GiB",
		Storage: "8 x 1000 SSD",
		VCpu:    "96",
		GPU:     "8",
		OnDemand: &AWSOfferTerm{
			Sku:           "H7NGEAC6UEHNTKSJ",
			OfferTermCode: "JRTCKXETXF",
			PriceDimensions: map[string]*AWSRateCode{
				"H7NGEAC6UEHNTKSJ.JRTCKXETXF.6YS6EN2CT7": {
					Unit: "Hrs",
					PricePerUnit: AWSCurrencyCode{
						USD: "32.7726000000",
					},
				},
			},
		},
	}

	expectedPricingCase1 := map[string]*AWSProductTerms{
		"us-east-1,p4d.24xlarge,linux":             expectedProdTermsInstanceOndemandCase1,
		"us-east-1,p4d.24xlarge,linux,preemptible": expectedProdTermsInstanceOndemandCase1,
	}

	if !reflect.DeepEqual(expectedPricingCase1, awsTest.Pricing) {
		expectedJsonString, _ := json.MarshalIndent(expectedPricingCase1, "", "  ")
		resultJsonString, _ := json.MarshalIndent(awsTest.Pricing, "", "  ")
		t.Logf("Expected: %s", string(expectedJsonString))
		t.Logf("Result: %s", string(resultJsonString))
		t.Fatalf("expected parsed pricing did not match actual parsed result (us-east-1)")
	}

	// Case 2
	awsTest = AWS{
		ValidPricingKeys: map[string]bool{},
	}

	fixture, err = os.Open("testdata/pricing-cn-northwest-1.json")
	if err != nil {
		t.Fatalf("failed to load pricing fixture: %s", err)
	}

	testResponse = http.Response{
		Body: io.NopCloser(fixture),
		Request: &http.Request{
			URL: &url.URL{
				Scheme: "https",
				Host:   "test-aws-http-endpoint:443",
			},
		},
	}

	awsTest.populatePricing(&testResponse, inputkeys)

	expectedProdTermsDisk = &AWSProductTerms{
		Sku:     "R83VXG9NAPDASEGN",
		Memory:  "",
		Storage: "",
		VCpu:    "",
		GPU:     "",
		OnDemand: &AWSOfferTerm{
			Sku:           "R83VXG9NAPDASEGN",
			OfferTermCode: "5Y9WH78GDR",
			PriceDimensions: map[string]*AWSRateCode{
				"R83VXG9NAPDASEGN.5Y9WH78GDR.Q7UJUT2CE6": {
					Unit: "GB-Mo",
					PricePerUnit: AWSCurrencyCode{
						USD: "",
						CNY: "0.5312000000",
					},
				},
			},
		},
		PV: &models.PV{
			Cost:       "0.0007276712328767123",
			CostPerIO:  "",
			Class:      "gp3",
			Size:       "",
			Region:     "cn-northwest-1",
			ProviderID: "",
		},
	}

	expectedPricing = map[string]*AWSProductTerms{
		"cn-northwest-1,EBS:VolumeUsage.gp3":             expectedProdTermsDisk,
		"cn-northwest-1,EBS:VolumeUsage.gp3,preemptible": expectedProdTermsDisk,
	}

	if !reflect.DeepEqual(expectedPricing, awsTest.Pricing) {
		t.Fatalf("expected parsed pricing did not match actual parsed result (cn)")
	}
}

func TestFeatures(t *testing.T) {
	testCases := map[string]struct {
		aws      awsKey
		expected string
	}{
		"Spot from custom labels": {
			aws: awsKey{
				SpotLabelName:  "node-type",
				SpotLabelValue: "node-spot",
				Labels: map[string]string{
					"node-type":                "node-spot",
					v1.LabelOSStable:           "linux",
					v1.LabelHostname:           "my-hostname",
					v1.LabelTopologyRegion:     "us-west-2",
					v1.LabelTopologyZone:       "us-west-2b",
					v1.LabelInstanceTypeStable: "m5.large",
				},
			},
			expected: "us-west-2,m5.large,linux,preemptible",
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			features := tc.aws.Features()
			if features != tc.expected {
				t.Errorf("expected %s, got %s", tc.expected, features)
			}
		})
	}
}

func Test_getStorageClassTypeFrom(t *testing.T) {
	tests := []struct {
		name        string
		provisioner string
		want        string
	}{
		{
			name:        "empty-provisioner",
			provisioner: "",
			want:        "",
		},
		{
			name:        "ebs-default-provisioner",
			provisioner: "kubernetes.io/aws-ebs",
			want:        "gp2",
		},
		{
			name:        "ebs-csi-provisioner",
			provisioner: "ebs.csi.aws.com",
			want:        "gp3",
		},
		{
			name:        "unknown-provisioner",
			provisioner: "unknown",
			want:        "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getStorageClassTypeFrom(tt.provisioner); got != tt.want {
				t.Errorf("getStorageClassTypeFrom() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_awsKey_isFargateNode(t *testing.T) {
	tests := []struct {
		name   string
		labels map[string]string
		want   bool
	}{
		{
			name: "fargate node with correct label",
			labels: map[string]string{
				eksComputeTypeLabel: "fargate",
			},
			want: true,
		},
		{
			name: "ec2 node with different compute type",
			labels: map[string]string{
				eksComputeTypeLabel: "ec2",
			},
			want: false,
		},
		{
			name: "node without compute type label",
			labels: map[string]string{
				"some.other.label": "value",
			},
			want: false,
		},
		{
			name:   "node with empty labels",
			labels: map[string]string{},
			want:   false,
		},
		{
			name:   "node with nil labels",
			labels: nil,
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := &awsKey{
				Labels: tt.labels,
			}
			if got := k.isFargateNode(); got != tt.want {
				t.Errorf("awsKey.isFargateNode() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetPricingListURL(t *testing.T) {
	tests := []struct {
		name        string
		serviceCode string
		nodeList    []*clustercache.Node
		expected    string
	}{
		{
			name:        "AmazonEC2 service with us-east-1 region",
			serviceCode: "AmazonEC2",
			nodeList: []*clustercache.Node{
				{
					Name: "test-node",
					Labels: map[string]string{
						"topology.kubernetes.io/region": "us-east-1",
					},
				},
			},
			expected: "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/us-east-1/index.json",
		},
		{
			name:        "AmazonECS service with us-west-2 region",
			serviceCode: "AmazonECS",
			nodeList: []*clustercache.Node{
				{
					Name: "test-node",
					Labels: map[string]string{
						"topology.kubernetes.io/region": "us-west-2",
					},
				},
			},
			expected: "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonECS/current/us-west-2/index.json",
		},
		{
			name:        "Chinese region cn-north-1",
			serviceCode: "AmazonEC2",
			nodeList: []*clustercache.Node{
				{
					Name: "test-node",
					Labels: map[string]string{
						"topology.kubernetes.io/region": "cn-north-1",
					},
				},
			},
			expected: "https://pricing.cn-north-1.amazonaws.com.cn/offers/v1.0/cn/AmazonEC2/current/cn-north-1/index.json",
		},
		{
			name:        "Chinese region cn-northwest-1",
			serviceCode: "AmazonECS",
			nodeList: []*clustercache.Node{
				{
					Name: "test-node",
					Labels: map[string]string{
						"topology.kubernetes.io/region": "cn-northwest-1",
					},
				},
			},
			expected: "https://pricing.cn-north-1.amazonaws.com.cn/offers/v1.0/cn/AmazonECS/current/cn-northwest-1/index.json",
		},
		{
			name:        "empty node list - multiregion",
			serviceCode: "AmazonEC2",
			nodeList:    []*clustercache.Node{},
			expected:    "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/index.json",
		},
		{
			name:        "multiple regions - multiregion",
			serviceCode: "AmazonECS",
			nodeList: []*clustercache.Node{
				{
					Name: "test-node-1",
					Labels: map[string]string{
						"topology.kubernetes.io/region": "us-east-1",
					},
				},
				{
					Name: "test-node-2",
					Labels: map[string]string{
						"topology.kubernetes.io/region": "us-west-2",
					},
				},
			},
			expected: "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonECS/current/index.json",
		},
		{
			name:        "node without region label",
			serviceCode: "AmazonEC2",
			nodeList: []*clustercache.Node{
				{
					Name: "test-node",
					Labels: map[string]string{
						"some.other.label": "value",
					},
				},
			},
			expected: "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/index.json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getPricingListURL(tt.serviceCode, tt.nodeList)
			if result != tt.expected {
				t.Errorf("getPricingListURL() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

func Test_configUpdaterWithReaderAndType_forSpotValues(t *testing.T) {
	fixture, err := os.Open("testdata/aws-config.json")
	if err != nil {
		t.Fatalf("failed to load aws config fixture: %s", err)
	}
	defer fixture.Close()
	c := &models.CustomPricing{}
	callback := configUpdaterWithReaderAndType(fixture, "otherupdatetype")
	err = callback(c)
	if err != nil {
		t.Fatalf("failed to load aws config: %s", err)
	}
	if c.AwsSpotDataBucket != "mybucket" {
		t.Fatalf("Expected %s but got %s", "mybucket", c.AwsSpotDataBucket)
	}
	if c.AwsSpotDataPrefix != "myprefix" {
		t.Fatalf("Expected %s but got %s", "myprefix", c.AwsSpotDataPrefix)
	}
	if c.AwsSpotDataRegion != "us-east-1" {
		t.Fatalf("Expected %s but got %s", "us-east-1", c.AwsSpotDataRegion)
	}

	fixture2, err := os.Open("testdata/aws-config-empty.json")
	if err != nil {
		t.Fatalf("failed to load aws config fixture: %s", err)
	}
	defer fixture2.Close()
	c = &models.CustomPricing{}
	callback = configUpdaterWithReaderAndType(fixture2, "otherupdatetype")
	err = callback(c)
	if err != nil {
		t.Fatalf("failed to load aws config: %s", err)
	}
	if c.AwsSpotDataBucket != "" {
		t.Fatalf("Expected empty string but got %s", c.AwsSpotDataBucket)
	}
	if c.AwsSpotDataPrefix != "" {
		t.Fatalf("Expected empty string but got %s", c.AwsSpotDataPrefix)
	}
	if c.AwsSpotDataRegion != "" {
		t.Fatalf("Expected empty string but got %s", c.AwsSpotDataRegion)
	}
}

// Mock cluster cache for testing
type mockClusterCache struct {
	pods []*clustercache.Pod
}

func (m *mockClusterCache) Run()  {}
func (m *mockClusterCache) Stop() {}

func (m *mockClusterCache) GetAllPods() []*clustercache.Pod {
	return m.pods
}

func (m *mockClusterCache) GetAllNodes() []*clustercache.Node {
	return nil
}

func (m *mockClusterCache) GetAllPersistentVolumes() []*clustercache.PersistentVolume {
	return nil
}

func (m *mockClusterCache) GetAllPersistentVolumeClaims() []*clustercache.PersistentVolumeClaim {
	return nil
}

func (m *mockClusterCache) GetAllStorageClasses() []*clustercache.StorageClass {
	return nil
}

func (m *mockClusterCache) GetAllServices() []*clustercache.Service {
	return nil
}

func (m *mockClusterCache) GetAllDeployments() []*clustercache.Deployment {
	return nil
}

func (m *mockClusterCache) GetAllDaemonSets() []*clustercache.DaemonSet {
	return nil
}

func (m *mockClusterCache) GetAllStatefulSets() []*clustercache.StatefulSet {
	return nil
}

func (m *mockClusterCache) GetAllReplicaSets() []*clustercache.ReplicaSet {
	return nil
}

func (m *mockClusterCache) GetAllJobs() []*clustercache.Job {
	return nil
}

func (m *mockClusterCache) GetAllNamespaces() []*clustercache.Namespace {
	return nil
}

func (m *mockClusterCache) GetAllPodDisruptionBudgets() []*clustercache.PodDisruptionBudget {
	return nil
}

func (m *mockClusterCache) GetAllReplicationControllers() []*clustercache.ReplicationController {
	return nil
}

func (m *mockClusterCache) GetAllResourceQuotas() []*clustercache.ResourceQuota {
	return nil
}

func TestAWS_getFargatePod(t *testing.T) {
	tests := []struct {
		name     string
		pods     []*clustercache.Pod
		awsKey   *awsKey
		wantPod  *clustercache.Pod
		wantBool bool
	}{
		{
			name: "pod found for node",
			pods: []*clustercache.Pod{
				{
					Name: "test-pod",
					Spec: clustercache.PodSpec{
						NodeName: "fargate-node-1",
					},
				},
			},
			awsKey: &awsKey{
				Name: "fargate-node-1",
			},
			wantPod: &clustercache.Pod{
				Name: "test-pod",
				Spec: clustercache.PodSpec{
					NodeName: "fargate-node-1",
				},
			},
			wantBool: true,
		},
		{
			name: "pod not found for node",
			pods: []*clustercache.Pod{
				{
					Name: "test-pod",
					Spec: clustercache.PodSpec{
						NodeName: "different-node",
					},
				},
			},
			awsKey: &awsKey{
				Name: "fargate-node-1",
			},
			wantPod:  nil,
			wantBool: false,
		},
		{
			name: "no pods in cluster",
			pods: []*clustercache.Pod{},
			awsKey: &awsKey{
				Name: "fargate-node-1",
			},
			wantPod:  nil,
			wantBool: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			aws := &AWS{
				Clientset: &mockClusterCache{pods: tt.pods},
			}

			gotPod, gotBool := aws.getFargatePod(tt.awsKey)

			if gotBool != tt.wantBool {
				t.Errorf("AWS.getFargatePod() gotBool = %v, want %v", gotBool, tt.wantBool)
			}

			if tt.wantPod == nil && gotPod != nil {
				t.Errorf("AWS.getFargatePod() gotPod = %v, want nil", gotPod)
			} else if tt.wantPod != nil && gotPod == nil {
				t.Errorf("AWS.getFargatePod() gotPod = nil, want %v", tt.wantPod)
			} else if tt.wantPod != nil && gotPod != nil {
				if gotPod.Name != tt.wantPod.Name || gotPod.Spec.NodeName != tt.wantPod.Spec.NodeName {
					t.Errorf("AWS.getFargatePod() gotPod = %v, want %v", gotPod, tt.wantPod)
				}
			}
		})
	}
}

// fakeProviderConfig implements models.ProviderConfig for testing
type fakeProviderConfig struct {
	customPricing *models.CustomPricing
}

func (f *fakeProviderConfig) GetCustomPricingData() (*models.CustomPricing, error) {
	if f.customPricing != nil {
		return f.customPricing, nil
	}
	return &models.CustomPricing{}, nil
}

func (f *fakeProviderConfig) Update(func(*models.CustomPricing) error) (*models.CustomPricing, error) {
	return f.customPricing, nil
}

func (f *fakeProviderConfig) UpdateFromMap(map[string]string) (*models.CustomPricing, error) {
	return f.customPricing, nil
}

func (f *fakeProviderConfig) ConfigFileManager() *config.ConfigFileManager {
	return nil
}

func TestAWS_SpotFeedRefreshEnabled(t *testing.T) {
	tests := []struct {
		name                string
		spotDataBucket      string
		spotDataRegion      string
		projectID           string
		spotDataFeedEnabled string
		want                bool
	}{
		{
			name:                "disabled via config - with bucket",
			spotDataBucket:      "my-bucket",
			spotDataRegion:      "us-east-1",
			projectID:           "123456789",
			spotDataFeedEnabled: "false",
			want:                false,
		},
		{
			name:                "disabled via config - with projectID only",
			projectID:           "123456789",
			spotDataFeedEnabled: "false",
			want:                false,
		},
		{
			name:                "enabled by default - with bucket",
			spotDataBucket:      "my-bucket",
			spotDataRegion:      "us-east-1",
			projectID:           "123456789",
			spotDataFeedEnabled: "",
			want:                true,
		},
		{
			name:                "enabled explicitly - with bucket",
			spotDataBucket:      "my-bucket",
			spotDataRegion:      "us-east-1",
			projectID:           "123456789",
			spotDataFeedEnabled: "true",
			want:                true,
		},
		{
			name:                "no spot config - disabled",
			spotDataBucket:      "",
			spotDataRegion:      "",
			projectID:           "",
			spotDataFeedEnabled: "",
			want:                false,
		},
		{
			name:                "no spot config - but explicitly enabled",
			spotDataBucket:      "",
			spotDataRegion:      "",
			projectID:           "",
			spotDataFeedEnabled: "true",
			want:                false,
		},
		{
			name:                "only projectID set - enabled by default",
			projectID:           "123456789",
			spotDataFeedEnabled: "",
			want:                true,
		},
		{
			name:                "only bucket set - enabled by default",
			spotDataBucket:      "my-bucket",
			spotDataFeedEnabled: "",
			want:                true,
		},
		{
			name:                "only region set - enabled by default",
			spotDataRegion:      "us-east-1",
			spotDataFeedEnabled: "",
			want:                true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			aws := &AWS{
				SpotDataBucket: tt.spotDataBucket,
				SpotDataRegion: tt.spotDataRegion,
				ProjectID:      tt.projectID,
				Config: &fakeProviderConfig{
					customPricing: &models.CustomPricing{
						SpotDataFeedEnabled: tt.spotDataFeedEnabled,
					},
				},
			}

			got := aws.SpotFeedRefreshEnabled()
			if got != tt.want {
				t.Errorf("AWS.SpotFeedRefreshEnabled() = %v, want %v", got, tt.want)
			}
		})
	}

	// Test nil Config scenario to ensure no panic
	t.Run("nil config - falls back to field check", func(t *testing.T) {
		aws := &AWS{
			SpotDataBucket: "my-bucket",
			SpotDataRegion: "us-east-1",
			ProjectID:      "123456789",
			Config:         nil, // nil Config should not cause panic
		}

		got := aws.SpotFeedRefreshEnabled()
		want := true // Should fall back to field-based check
		if got != want {
			t.Errorf("AWS.SpotFeedRefreshEnabled() with nil Config = %v, want %v", got, want)
		}
	})

	t.Run("nil config - no spot fields", func(t *testing.T) {
		aws := &AWS{
			SpotDataBucket: "",
			SpotDataRegion: "",
			ProjectID:      "",
			Config:         nil, // nil Config should not cause panic
		}

		got := aws.SpotFeedRefreshEnabled()
		want := false // No fields set, should return false
		if got != want {
			t.Errorf("AWS.SpotFeedRefreshEnabled() with nil Config and no fields = %v, want %v", got, want)
		}
	})
}

func TestAWS_spotPricingFromHistory(t *testing.T) {
	t.Run("nil cache returns false", func(t *testing.T) {
		aws := &AWS{}
		key := &awsKey{
			ProviderID: "aws:///us-east-1a/i-0123456789abcdef0",
			Labels: map[string]string{
				"topology.kubernetes.io/region":    "us-east-1",
				"topology.kubernetes.io/zone":      "us-east-1a",
				"node.kubernetes.io/instance-type": "m5.large",
				"kubernetes.io/os":                 "linux",
				"eks.amazonaws.com/capacityType":   "SPOT",
			},
		}
		_, ok := aws.spotPricingFromHistory(key)
		if ok {
			t.Error("Expected false when cache is nil")
		}
	})

	t.Run("missing region label returns false", func(t *testing.T) {
		mockFetcher := &mockSpotPriceHistoryFetcher{}
		aws := &AWS{
			SpotPriceHistoryCache: NewSpotPriceHistoryCache(mockFetcher),
		}
		key := &awsKey{
			ProviderID: "aws:///us-east-1a/i-0123456789abcdef0",
			Labels: map[string]string{
				"topology.kubernetes.io/zone":      "us-east-1a",
				"node.kubernetes.io/instance-type": "m5.large",
			},
		}
		_, ok := aws.spotPricingFromHistory(key)
		if ok {
			t.Error("Expected false when region label is missing")
		}
	})

	t.Run("missing instance type label returns false", func(t *testing.T) {
		mockFetcher := &mockSpotPriceHistoryFetcher{}
		aws := &AWS{
			SpotPriceHistoryCache: NewSpotPriceHistoryCache(mockFetcher),
		}
		key := &awsKey{
			ProviderID: "aws:///us-east-1a/i-0123456789abcdef0",
			Labels: map[string]string{
				"topology.kubernetes.io/region": "us-east-1",
				"topology.kubernetes.io/zone":   "us-east-1a",
			},
		}
		_, ok := aws.spotPricingFromHistory(key)
		if ok {
			t.Error("Expected false when instance type label is missing")
		}
	})

	t.Run("missing zone label returns false", func(t *testing.T) {
		mockFetcher := &mockSpotPriceHistoryFetcher{}
		aws := &AWS{
			SpotPriceHistoryCache: NewSpotPriceHistoryCache(mockFetcher),
		}
		key := &awsKey{
			ProviderID: "aws:///us-east-1a/i-0123456789abcdef0",
			Labels: map[string]string{
				"topology.kubernetes.io/region":    "us-east-1",
				"node.kubernetes.io/instance-type": "m5.large",
			},
		}
		_, ok := aws.spotPricingFromHistory(key)
		if ok {
			t.Error("Expected false when zone label is missing")
		}
	})

	t.Run("fetcher error returns false", func(t *testing.T) {
		mockFetcher := &mockSpotPriceHistoryFetcher{
			fetchFunc: func(key SpotPriceHistoryKey) (*SpotPriceHistoryEntry, error) {
				return nil, errors.New("api error")
			},
		}
		aws := &AWS{
			SpotPriceHistoryCache: NewSpotPriceHistoryCache(mockFetcher),
		}
		key := &awsKey{
			ProviderID: "aws:///us-east-1a/i-0123456789abcdef0",
			Labels: map[string]string{
				"topology.kubernetes.io/region":    "us-east-1",
				"topology.kubernetes.io/zone":      "us-east-1a",
				"node.kubernetes.io/instance-type": "m5.large",
			},
		}
		_, ok := aws.spotPricingFromHistory(key)
		if ok {
			t.Error("Expected false when fetcher returns error")
		}
	})

	t.Run("successful lookup returns entry", func(t *testing.T) {
		mockFetcher := &mockSpotPriceHistoryFetcher{
			fetchFunc: func(key SpotPriceHistoryKey) (*SpotPriceHistoryEntry, error) {
				if key.Region != "us-east-1" || key.InstanceType != "m5.large" || key.AvailabilityZone != "us-east-1a" {
					t.Errorf("Unexpected key: %v", key)
				}
				return &SpotPriceHistoryEntry{
					SpotPrice:   0.042,
					Timestamp:   time.Now(),
					RetrievedAt: time.Now(),
				}, nil
			},
		}
		aws := &AWS{
			SpotPriceHistoryCache: NewSpotPriceHistoryCache(mockFetcher),
		}
		key := &awsKey{
			ProviderID: "aws:///us-east-1a/i-0123456789abcdef0",
			Labels: map[string]string{
				"topology.kubernetes.io/region":    "us-east-1",
				"topology.kubernetes.io/zone":      "us-east-1a",
				"node.kubernetes.io/instance-type": "m5.large",
			},
		}
		entry, ok := aws.spotPricingFromHistory(key)
		if !ok {
			t.Fatal("Expected true for successful lookup")
		}
		if entry.SpotPrice != 0.042 {
			t.Errorf("Expected spot price 0.042, got %f", entry.SpotPrice)
		}
	})
}

func TestAWS_createNode_spotHistoryFallback(t *testing.T) {
	// Helper to build AWSProductTerms with on-demand pricing
	makeTerms := func(sku, offerTermCode, cost string) *AWSProductTerms {
		priceKey := sku + "." + offerTermCode + "." + HourlyRateCode
		return &AWSProductTerms{
			Sku: sku,
			OnDemand: &AWSOfferTerm{
				Sku:           sku,
				OfferTermCode: offerTermCode,
				PriceDimensions: map[string]*AWSRateCode{
					priceKey: {
						Unit:         "Hrs",
						PricePerUnit: AWSCurrencyCode{USD: cost},
					},
				},
			},
			VCpu:   "4",
			Memory: "16",
		}
	}

	t.Run("preemptible node uses spot history when available", func(t *testing.T) {
		mockFetcher := &mockSpotPriceHistoryFetcher{
			fetchFunc: func(key SpotPriceHistoryKey) (*SpotPriceHistoryEntry, error) {
				return &SpotPriceHistoryEntry{
					SpotPrice:   0.035,
					Timestamp:   time.Now(),
					RetrievedAt: time.Now(),
				}, nil
			},
		}
		aws := &AWS{
			SpotPriceHistoryCache: NewSpotPriceHistoryCache(mockFetcher),
			BaseCPUPrice:          "0.04",
			BaseRAMPrice:          "0.01",
			BaseGPUPrice:          "0.95",
		}
		terms := makeTerms("SKU123", "JRTCKXETXF", "0.096")
		// Key with PreemptibleType suffix to trigger isPreemptible
		key := &awsKey{
			ProviderID:     "aws:///us-east-1a/i-0123456789abcdef0",
			SpotLabelName:  "eks.amazonaws.com/capacityType",
			SpotLabelValue: "SPOT",
			Labels: map[string]string{
				"topology.kubernetes.io/region":    "us-east-1",
				"topology.kubernetes.io/zone":      "us-east-1a",
				"node.kubernetes.io/instance-type": "m5.large",
				"kubernetes.io/os":                 "linux",
				"eks.amazonaws.com/capacityType":   "SPOT",
			},
		}

		node, meta, err := aws.createNode(terms, PreemptibleType, key)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if node.Cost != "0.035000" {
			t.Errorf("Expected spot history cost 0.035000, got %s", node.Cost)
		}
		if node.UsageType != PreemptibleType {
			t.Errorf("Expected usage type %s, got %s", PreemptibleType, node.UsageType)
		}
		if meta.Source != SpotPriceHistorySource {
			t.Errorf("Expected source %s, got %s", SpotPriceHistorySource, meta.Source)
		}
	})

	t.Run("preemptible node falls back to on-demand when history unavailable", func(t *testing.T) {
		mockFetcher := &mockSpotPriceHistoryFetcher{
			fetchFunc: func(key SpotPriceHistoryKey) (*SpotPriceHistoryEntry, error) {
				return nil, errors.New("no data")
			},
		}
		aws := &AWS{
			SpotPriceHistoryCache: NewSpotPriceHistoryCache(mockFetcher),
			BaseCPUPrice:          "0.04",
			BaseRAMPrice:          "0.01",
			BaseGPUPrice:          "0.95",
		}
		terms := makeTerms("SKU123", "JRTCKXETXF", "0.096")
		key := &awsKey{
			ProviderID:     "aws:///us-east-1a/i-0123456789abcdef0",
			SpotLabelName:  "eks.amazonaws.com/capacityType",
			SpotLabelValue: "SPOT",
			Labels: map[string]string{
				"topology.kubernetes.io/region":    "us-east-1",
				"topology.kubernetes.io/zone":      "us-east-1a",
				"node.kubernetes.io/instance-type": "m5.large",
				"kubernetes.io/os":                 "linux",
				"eks.amazonaws.com/capacityType":   "SPOT",
			},
		}

		node, _, err := aws.createNode(terms, PreemptibleType, key)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if node.Cost != "0.096" {
			t.Errorf("Expected on-demand cost 0.096, got %s", node.Cost)
		}
		if node.UsageType != PreemptibleType {
			t.Errorf("Expected usage type %s, got %s", PreemptibleType, node.UsageType)
		}
	})

	t.Run("preemptible node with nil cache falls back to on-demand", func(t *testing.T) {
		aws := &AWS{
			BaseCPUPrice: "0.04",
			BaseRAMPrice: "0.01",
			BaseGPUPrice: "0.95",
		}
		terms := makeTerms("SKU123", "JRTCKXETXF", "0.096")
		key := &awsKey{
			ProviderID:     "aws:///us-east-1a/i-0123456789abcdef0",
			SpotLabelName:  "eks.amazonaws.com/capacityType",
			SpotLabelValue: "SPOT",
			Labels: map[string]string{
				"topology.kubernetes.io/region":    "us-east-1",
				"topology.kubernetes.io/zone":      "us-east-1a",
				"node.kubernetes.io/instance-type": "m5.large",
				"kubernetes.io/os":                 "linux",
				"eks.amazonaws.com/capacityType":   "SPOT",
			},
		}

		node, _, err := aws.createNode(terms, PreemptibleType, key)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if node.Cost != "0.096" {
			t.Errorf("Expected on-demand cost 0.096, got %s", node.Cost)
		}
	})

	t.Run("preemptible node uses base spot prices when no public pricing", func(t *testing.T) {
		mockFetcher := &mockSpotPriceHistoryFetcher{
			fetchFunc: func(key SpotPriceHistoryKey) (*SpotPriceHistoryEntry, error) {
				return nil, errors.New("no data")
			},
		}
		aws := &AWS{
			SpotPriceHistoryCache: NewSpotPriceHistoryCache(mockFetcher),
			BaseCPUPrice:          "0.04",
			BaseRAMPrice:          "0.01",
			BaseGPUPrice:          "0.95",
			BaseSpotCPUPrice:      "0.02",
			BaseSpotRAMPrice:      "0.005",
		}
		// Terms without valid pricing dimensions
		terms := &AWSProductTerms{
			Sku: "SKU123",
			OnDemand: &AWSOfferTerm{
				Sku:             "SKU123",
				OfferTermCode:   "JRTCKXETXF",
				PriceDimensions: map[string]*AWSRateCode{},
			},
			VCpu:   "4",
			Memory: "16",
		}
		key := &awsKey{
			ProviderID:     "aws:///us-east-1a/i-0123456789abcdef0",
			SpotLabelName:  "eks.amazonaws.com/capacityType",
			SpotLabelValue: "SPOT",
			Labels: map[string]string{
				"topology.kubernetes.io/region":    "us-east-1",
				"topology.kubernetes.io/zone":      "us-east-1a",
				"node.kubernetes.io/instance-type": "m5.large",
				"kubernetes.io/os":                 "linux",
				"eks.amazonaws.com/capacityType":   "SPOT",
			},
		}

		node, _, err := aws.createNode(terms, PreemptibleType, key)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if node.VCPUCost != "0.02" {
			t.Errorf("Expected base spot CPU price 0.02, got %s", node.VCPUCost)
		}
		if node.RAMCost != "0.005" {
			t.Errorf("Expected base spot RAM price 0.005, got %s", node.RAMCost)
		}
	})
}

func TestAWS_PricingSourceStatus_spotPriceHistory(t *testing.T) {
	t.Run("not yet initialized", func(t *testing.T) {
		aws := &AWS{
			Config: &fakeProviderConfig{
				customPricing: &models.CustomPricing{},
			},
		}
		sources := aws.PricingSourceStatus()
		sphs, ok := sources[SpotPriceHistorySource]
		if !ok {
			t.Fatal("Expected SpotPriceHistorySource in sources")
		}
		if sphs.Available {
			t.Error("Expected Available=false when cache not initialized")
		}
		if sphs.Error != "Not yet initialized" {
			t.Errorf("Expected 'Not yet initialized' error, got %q", sphs.Error)
		}
	})

	t.Run("initialization error", func(t *testing.T) {
		aws := &AWS{
			SpotPriceHistoryError: errors.New("no cluster region configured"),
			Config: &fakeProviderConfig{
				customPricing: &models.CustomPricing{},
			},
		}
		sources := aws.PricingSourceStatus()
		sphs := sources[SpotPriceHistorySource]
		if sphs.Available {
			t.Error("Expected Available=false on error")
		}
		if sphs.Error != "no cluster region configured" {
			t.Errorf("Expected error message, got %q", sphs.Error)
		}
	})

	t.Run("successfully initialized", func(t *testing.T) {
		mockFetcher := &mockSpotPriceHistoryFetcher{}
		aws := &AWS{
			SpotPriceHistoryCache: NewSpotPriceHistoryCache(mockFetcher),
			Config: &fakeProviderConfig{
				customPricing: &models.CustomPricing{},
			},
		}
		sources := aws.PricingSourceStatus()
		sphs := sources[SpotPriceHistorySource]
		if !sphs.Available {
			t.Error("Expected Available=true when cache initialized")
		}
	})
}
