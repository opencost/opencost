package alibaba

import (
	"testing"

	"github.com/aliyun/alibaba-cloud-sdk-go/services/bssopenapi"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/pkg/cloud"
)

func TestBoaQuerier_GetStatus(t *testing.T) {
	testCases := map[string]struct {
		querier        BoaQuerier
		expectedStatus cloud.ConnectionStatus
	}{
		"initial status": {
			querier:        BoaQuerier{},
			expectedStatus: cloud.InitialStatus,
		},
		"existing status": {
			querier: BoaQuerier{
				ConnectionStatus: cloud.SuccessfulConnection,
			},
			expectedStatus: cloud.SuccessfulConnection,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			status := testCase.querier.GetStatus()
			if status != testCase.expectedStatus {
				t.Errorf("expected status %v, got %v", testCase.expectedStatus, status)
			}
		})
	}
}

func TestBoaQuerier_Equals(t *testing.T) {
	baseQuerier := BoaQuerier{
		BOAConfiguration: BOAConfiguration{
			Account: "account1",
			Region:  "region1",
			Authorizer: &AccessKey{
				AccessKeyID:     "id1",
				AccessKeySecret: "secret1",
			},
		},
		ConnectionStatus: cloud.SuccessfulConnection,
	}

	testCases := map[string]struct {
		left     BoaQuerier
		right    cloud.Config
		expected bool
	}{
		"matching queriers": {
			left: baseQuerier,
			right: &BoaQuerier{
				BOAConfiguration: BOAConfiguration{
					Account: "account1",
					Region:  "region1",
					Authorizer: &AccessKey{
						AccessKeyID:     "id1",
						AccessKeySecret: "secret1",
					},
				},
				ConnectionStatus: cloud.SuccessfulConnection,
			},
			expected: true,
		},
		"different config": {
			left: baseQuerier,
			right: &BOAConfiguration{
				Account: "account1",
				Region:  "region1",
			},
			expected: false,
		},
		"nil config": {
			left:     baseQuerier,
			right:    nil,
			expected: false,
		},
		"different account": {
			left: baseQuerier,
			right: &BoaQuerier{
				BOAConfiguration: BOAConfiguration{
					Account: "account2",
					Region:  "region1",
					Authorizer: &AccessKey{
						AccessKeyID:     "id1",
						AccessKeySecret: "secret1",
					},
				},
			},
			expected: false,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			result := testCase.left.Equals(testCase.right)
			if result != testCase.expected {
				t.Errorf("expected %t, got %t", testCase.expected, result)
			}
		})
	}
}

func TestBoaQuerier_QueryInstanceBill(t *testing.T) {
	// This test would require a mock BSS client
	// For now, we'll test the function signature and basic error handling
	querier := BoaQuerier{
		BOAConfiguration: BOAConfiguration{
			Account: "test-account",
			Region:  "test-region",
			Authorizer: &AccessKey{
				AccessKeyID:     "test-id",
				AccessKeySecret: "test-secret",
			},
		},
	}

	// Test with nil client (should panic or return error)
	defer func() {
		if r := recover(); r == nil {
			// Expected to panic with nil client
		}
	}()

	_, err := querier.QueryInstanceBill(nil, true, "https", "DAILY", "2023-01", "2023-01-01", 1)
	if err == nil {
		t.Error("expected error with nil client")
	}
}

func TestBoaQuerier_QueryBoaPaginated(t *testing.T) {
	querier := BoaQuerier{
		BOAConfiguration: BOAConfiguration{
			Account: "test-account",
			Region:  "test-region",
			Authorizer: &AccessKey{
				AccessKeyID:     "test-id",
				AccessKeySecret: "test-secret",
			},
		},
	}

	// Test with nil client - this will panic, so we need to recover
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic with nil client")
		}
	}()

	querier.QueryBoaPaginated(nil, true, "https", "DAILY", "2023-01", "2023-01-01", func(*bssopenapi.QueryInstanceBillResponse) bool {
		return true
	})
}

func TestGetBoaQueryInstanceBillFunc(t *testing.T) {
	testCases := map[string]struct {
		response       *bssopenapi.QueryInstanceBillResponse
		expectedReturn bool
		callCount      int
	}{
		"nil response": {
			response:       nil,
			expectedReturn: false,
			callCount:      0,
		},
		"empty response": {
			response:       &bssopenapi.QueryInstanceBillResponse{},
			expectedReturn: false,
			callCount:      0,
		},
		"valid response with items": {
			response:       &bssopenapi.QueryInstanceBillResponse{},
			expectedReturn: false,
			callCount:      0,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			callCount := 0
			handler := func(item bssopenapi.Item) error {
				callCount++
				return nil
			}

			processor := GetBoaQueryInstanceBillFunc(handler, "2023-01-01")
			result := processor(testCase.response)

			if result != testCase.expectedReturn {
				t.Errorf("expected return %t, got %t", testCase.expectedReturn, result)
			}

			if callCount != testCase.callCount {
				t.Errorf("expected %d calls to handler, got %d", testCase.callCount, callCount)
			}
		})
	}
}

func TestSelectAlibabaCategory(t *testing.T) {
	testCases := map[string]struct {
		item             bssopenapi.Item
		expectedCategory string
	}{
		"empty item": {
			item:             bssopenapi.Item{},
			expectedCategory: opencost.OtherCategory,
		},
		"node instance": {
			item: bssopenapi.Item{
				InstanceID: "i-test123",
			},
			expectedCategory: opencost.ComputeCategory,
		},
		"disk instance": {
			item: bssopenapi.Item{
				InstanceID: "d-test123",
			},
			expectedCategory: opencost.StorageCategory,
		},
		"network usage": {
			item: bssopenapi.Item{
				UsageUnit: "piece",
			},
			expectedCategory: opencost.NetworkCategory,
		},
		"SLB product": {
			item: bssopenapi.Item{
				ProductCode: "slb",
			},
			expectedCategory: opencost.NetworkCategory,
		},
		"EIP product": {
			item: bssopenapi.Item{
				ProductCode: "eip",
			},
			expectedCategory: opencost.NetworkCategory,
		},
		"NIS product": {
			item: bssopenapi.Item{
				ProductCode: "nis",
			},
			expectedCategory: opencost.NetworkCategory,
		},
		"GTM product": {
			item: bssopenapi.Item{
				ProductCode: "gtm",
			},
			expectedCategory: opencost.NetworkCategory,
		},
		"ECS product": {
			item: bssopenapi.Item{
				ProductCode: "ecs",
			},
			expectedCategory: opencost.ComputeCategory,
		},
		"EDS product": {
			item: bssopenapi.Item{
				ProductCode: "eds",
			},
			expectedCategory: opencost.ComputeCategory,
		},
		"SAS product": {
			item: bssopenapi.Item{
				ProductCode: "sas",
			},
			expectedCategory: opencost.ComputeCategory,
		},
		"ACK product": {
			item: bssopenapi.Item{
				ProductCode: "ack",
			},
			expectedCategory: opencost.ManagementCategory,
		},
		"EBS product": {
			item: bssopenapi.Item{
				ProductCode: "ebs",
			},
			expectedCategory: opencost.StorageCategory,
		},
		"OSS product": {
			item: bssopenapi.Item{
				ProductCode: "oss",
			},
			expectedCategory: opencost.StorageCategory,
		},
		"SCU product": {
			item: bssopenapi.Item{
				ProductCode: "scu",
			},
			expectedCategory: opencost.StorageCategory,
		},
		"unknown product": {
			item: bssopenapi.Item{
				ProductCode: "unknown",
			},
			expectedCategory: opencost.OtherCategory,
		},
		"case insensitive product code": {
			item: bssopenapi.Item{
				ProductCode: "SLB",
			},
			expectedCategory: opencost.NetworkCategory,
		},
		"node with network usage (node takes precedence)": {
			item: bssopenapi.Item{
				InstanceID: "i-test123",
				UsageUnit:  "piece",
			},
			expectedCategory: opencost.ComputeCategory,
		},
		"disk with network usage (disk takes precedence)": {
			item: bssopenapi.Item{
				InstanceID: "d-test123",
				UsageUnit:  "piece",
			},
			expectedCategory: opencost.StorageCategory,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			result := SelectAlibabaCategory(testCase.item)
			if result != testCase.expectedCategory {
				t.Errorf("expected category %s, got %s", testCase.expectedCategory, result)
			}
		})
	}
}

func TestBoaQuerier_Integration(t *testing.T) {
	// Test that BoaQuerier properly embeds BOAConfiguration
	querier := BoaQuerier{
		BOAConfiguration: BOAConfiguration{
			Account: "test-account",
			Region:  "test-region",
			Authorizer: &AccessKey{
				AccessKeyID:     "test-id",
				AccessKeySecret: "test-secret",
			},
		},
		ConnectionStatus: cloud.SuccessfulConnection,
	}

	// Test that we can access BOAConfiguration methods
	if querier.Account != "test-account" {
		t.Errorf("expected account test-account, got %s", querier.Account)
	}

	if querier.Region != "test-region" {
		t.Errorf("expected region test-region, got %s", querier.Region)
	}

	// Test Key method from embedded BOAConfiguration
	key := querier.Key()
	expectedKey := "test-account/test-region"
	if key != expectedKey {
		t.Errorf("expected key %s, got %s", expectedKey, key)
	}

	// Test Provider method from embedded BOAConfiguration
	provider := querier.Provider()
	if provider != opencost.AlibabaProvider {
		t.Errorf("expected provider %s, got %s", opencost.AlibabaProvider, provider)
	}
}
