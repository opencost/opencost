package azure

import (
	"fmt"
	"testing"

	"github.com/opencost/opencost/pkg/cloud/config"
	"github.com/opencost/opencost/pkg/log"
	"github.com/opencost/opencost/pkg/util/json"
)

func TestStorageConfiguration_Validate(t *testing.T) {
	testCases := map[string]struct {
		config   StorageConfiguration
		expected error
	}{
		"valid config Azure AccessKey": {
			config: StorageConfiguration{
				SubscriptionID: "subscriptionID",
				Account:        "account",
				Container:      "container",
				Path:           "path",
				Cloud:          "cloud",
				Authorizer: &AccessKey{
					AccessKey: "accessKey",
					Account:   "account",
				},
			},
			expected: nil,
		},
		"access key invalid": {
			config: StorageConfiguration{
				SubscriptionID: "subscriptionID",
				Account:        "account",
				Container:      "container",
				Path:           "path",
				Cloud:          "cloud",
				Authorizer: &AccessKey{
					Account: "account",
				},
			},
			expected: fmt.Errorf("AccessKey: missing access key"),
		},
		"missing authorizer": {
			config: StorageConfiguration{
				SubscriptionID: "subscriptionID",
				Account:        "account",
				Container:      "container",
				Path:           "path",
				Cloud:          "cloud",
				Authorizer:     nil,
			},
			expected: fmt.Errorf("StorageConfiguration: missing authorizer"),
		},
		"missing subscriptionID": {
			config: StorageConfiguration{
				SubscriptionID: "",
				Account:        "account",
				Container:      "container",
				Path:           "path",
				Cloud:          "cloud",
				Authorizer: &AccessKey{
					AccessKey: "accessKey",
					Account:   "account",
				},
			},
			expected: fmt.Errorf("StorageConfiguration: missing Subcription ID"),
		},
		"missing account": {
			config: StorageConfiguration{
				SubscriptionID: "subscriptionID",
				Account:        "",
				Container:      "container",
				Path:           "path",
				Cloud:          "cloud",
				Authorizer: &AccessKey{
					AccessKey: "accessKey",
					Account:   "account",
				},
			},
			expected: fmt.Errorf("StorageConfiguration: missing Account"),
		},
		"missing container": {
			config: StorageConfiguration{
				SubscriptionID: "subscriptionID",
				Account:        "account",
				Container:      "",
				Path:           "path",
				Cloud:          "cloud",
				Authorizer: &AccessKey{
					AccessKey: "accessKey",
					Account:   "account",
				},
			},
			expected: fmt.Errorf("StorageConfiguration: missing Container"),
		},
		"missing path": {
			config: StorageConfiguration{
				SubscriptionID: "subscriptionID",
				Account:        "account",
				Container:      "container",
				Path:           "",
				Cloud:          "cloud",
				Authorizer: &AccessKey{
					AccessKey: "accessKey",
					Account:   "account",
				},
			},
			expected: nil,
		},
		"missing cloud": {
			config: StorageConfiguration{
				SubscriptionID: "subscriptionID",
				Account:        "account",
				Container:      "container",
				Path:           "path",
				Cloud:          "",
				Authorizer: &AccessKey{
					AccessKey: "accessKey",
					Account:   "account",
				},
			},
			expected: nil,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			actual := testCase.config.Validate()
			actualString := "nil"
			if actual != nil {
				actualString = actual.Error()
			}
			expectedString := "nil"
			if testCase.expected != nil {
				expectedString = testCase.expected.Error()
			}
			if actualString != expectedString {
				t.Errorf("errors do not match: Actual: '%s', Expected: '%s", actualString, expectedString)
			}
		})
	}
}

func TestStorageConfiguration_Equals(t *testing.T) {
	testCases := map[string]struct {
		left     StorageConfiguration
		right    config.Config
		expected bool
	}{
		"matching config": {
			left: StorageConfiguration{
				SubscriptionID: "subscriptionID",
				Account:        "account",
				Container:      "container",
				Path:           "path",
				Cloud:          "cloud",
				Authorizer: &AccessKey{
					AccessKey: "accessKey",
					Account:   "account",
				},
			},
			right: &StorageConfiguration{
				SubscriptionID: "subscriptionID",
				Account:        "account",
				Container:      "container",
				Path:           "path",
				Cloud:          "cloud",
				Authorizer: &AccessKey{
					AccessKey: "accessKey",
					Account:   "account",
				},
			},
			expected: true,
		},

		"missing both authorizer": {
			left: StorageConfiguration{
				SubscriptionID: "subscriptionID",
				Account:        "account",
				Container:      "container",
				Path:           "path",
				Cloud:          "cloud",
				Authorizer:     nil,
			},
			right: &StorageConfiguration{
				SubscriptionID: "subscriptionID",
				Account:        "account",
				Container:      "container",
				Path:           "path",
				Cloud:          "cloud",
				Authorizer:     nil,
			},
			expected: true,
		},
		"missing left authorizer": {
			left: StorageConfiguration{
				SubscriptionID: "subscriptionID",
				Account:        "account",
				Container:      "container",
				Path:           "path",
				Cloud:          "cloud",
				Authorizer:     nil,
			},
			right: &StorageConfiguration{
				SubscriptionID: "subscriptionID",
				Account:        "account",
				Container:      "container",
				Path:           "path",
				Cloud:          "cloud",
				Authorizer: &AccessKey{
					AccessKey: "accessKey",
					Account:   "account",
				},
			},
			expected: false,
		},
		"missing right authorizer": {
			left: StorageConfiguration{
				SubscriptionID: "subscriptionID",
				Account:        "account",
				Container:      "container",
				Path:           "path",
				Cloud:          "cloud",
				Authorizer: &AccessKey{
					AccessKey: "accessKey",
					Account:   "account",
				},
			},
			right: &StorageConfiguration{
				SubscriptionID: "subscriptionID",
				Account:        "account",
				Container:      "container",
				Path:           "path",
				Cloud:          "cloud",
				Authorizer:     nil,
			},
			expected: false,
		},
		"different subscriptionID": {
			left: StorageConfiguration{
				SubscriptionID: "subscriptionID",
				Account:        "account",
				Container:      "container",
				Path:           "path",
				Cloud:          "cloud",
				Authorizer: &AccessKey{
					AccessKey: "accessKey",
					Account:   "account",
				},
			},
			right: &StorageConfiguration{
				SubscriptionID: "subscriptionID2",
				Account:        "account",
				Container:      "container",
				Path:           "path",
				Cloud:          "cloud",
				Authorizer: &AccessKey{
					AccessKey: "accessKey",
					Account:   "account",
				},
			},
			expected: false,
		},
		"different account": {
			left: StorageConfiguration{
				SubscriptionID: "subscriptionID",
				Account:        "account",
				Container:      "container",
				Path:           "path",
				Cloud:          "cloud",
				Authorizer: &AccessKey{
					AccessKey: "accessKey",
					Account:   "account",
				},
			},
			right: &StorageConfiguration{
				SubscriptionID: "subscriptionID",
				Account:        "account2",
				Container:      "container",
				Path:           "path",
				Cloud:          "cloud",
				Authorizer: &AccessKey{
					AccessKey: "accessKey",
					Account:   "account",
				},
			},
			expected: false,
		},
		"different container": {
			left: StorageConfiguration{
				SubscriptionID: "subscriptionID",
				Account:        "account",
				Container:      "container",
				Path:           "path",
				Cloud:          "cloud",
				Authorizer: &AccessKey{
					AccessKey: "accessKey",
					Account:   "account",
				},
			},
			right: &StorageConfiguration{
				SubscriptionID: "subscriptionID",
				Account:        "account",
				Container:      "container2",
				Path:           "path",
				Cloud:          "cloud",
				Authorizer: &AccessKey{
					AccessKey: "accessKey",
					Account:   "account",
				},
			},
			expected: false,
		},
		"different path": {
			left: StorageConfiguration{
				SubscriptionID: "subscriptionID",
				Account:        "account",
				Container:      "container",
				Path:           "path",
				Cloud:          "cloud",
				Authorizer: &AccessKey{
					AccessKey: "accessKey",
					Account:   "account",
				},
			},
			right: &StorageConfiguration{
				SubscriptionID: "subscriptionID",
				Account:        "account",
				Container:      "container",
				Path:           "path2",
				Cloud:          "cloud",
				Authorizer: &AccessKey{
					AccessKey: "accessKey",
					Account:   "account",
				},
			},
			expected: false,
		},
		"different cloud": {
			left: StorageConfiguration{
				SubscriptionID: "subscriptionID",
				Account:        "account",
				Container:      "container",
				Path:           "path",
				Cloud:          "cloud",
				Authorizer: &AccessKey{
					AccessKey: "accessKey",
					Account:   "account",
				},
			},
			right: &StorageConfiguration{
				SubscriptionID: "subscriptionID",
				Account:        "account",
				Container:      "container",
				Path:           "path",
				Cloud:          "cloud2",
				Authorizer: &AccessKey{
					AccessKey: "accessKey",
					Account:   "account",
				},
			},
			expected: false,
		},
		"different config": {
			left: StorageConfiguration{
				SubscriptionID: "subscriptionID",
				Account:        "account",
				Container:      "container",
				Path:           "path",
				Cloud:          "cloud",
				Authorizer: &AccessKey{
					AccessKey: "accessKey",
					Account:   "account",
				},
			},
			right: &AccessKey{
				AccessKey: "accessKey",
				Account:   "account",
			},
			expected: false,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			actual := testCase.left.Equals(testCase.right)
			if actual != testCase.expected {
				t.Errorf("incorrect result: Actual: '%t', Expected: '%t", actual, testCase.expected)
			}
		})
	}
}

func TestStorageConfiguration_JSON(t *testing.T) {
	testCases := map[string]struct {
		config StorageConfiguration
	}{
		"Empty Config": {
			config: StorageConfiguration{},
		},
		"Nil Authorizer": {
			config: StorageConfiguration{
				SubscriptionID: "subscriptionID",
				Account:        "account",
				Container:      "container",
				Path:           "path",
				Cloud:          "cloud",
				Authorizer:     nil,
			},
		},
		"AccessKey Authorizer": {
			config: StorageConfiguration{
				SubscriptionID: "subscriptionID",
				Account:        "account",
				Container:      "container",
				Path:           "path",
				Cloud:          "cloud",
				Authorizer: &AccessKey{
					AccessKey: "accessKey",
					Account:   "account",
				},
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			// test JSON Marshalling
			configJSON, err := json.Marshal(testCase.config)
			if err != nil {
				t.Errorf("failed to marshal configuration: %s", err.Error())
			}
			log.Info(string(configJSON))
			unmarshalledConfig := &StorageConfiguration{}
			err = json.Unmarshal(configJSON, unmarshalledConfig)
			if err != nil {
				t.Errorf("failed to unmarshal configuration: %s", err.Error())
			}

			if !testCase.config.Equals(unmarshalledConfig) {
				t.Error("config does not equal unmarshalled config")
			}
		})
	}
}
