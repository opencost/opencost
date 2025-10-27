package alibaba

import (
	"fmt"
	"testing"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/pkg/cloud"
)

func TestBoaConfiguration_Validate(t *testing.T) {
	testCases := map[string]struct {
		config   BOAConfiguration
		expected error
	}{
		"valid config Azure AccessKey": {
			config: BOAConfiguration{
				Account: "Account Number",
				Region:  "Region",
				Authorizer: &AccessKey{
					AccessKeyID:     "accessKeyID",
					AccessKeySecret: "accessKeySecret",
				},
			},
			expected: nil,
		},
		"access key invalid": {
			config: BOAConfiguration{
				Account: "Account Number",
				Region:  "Region",
				Authorizer: &AccessKey{
					AccessKeySecret: "accessKeySecret",
				},
			},
			expected: fmt.Errorf("AccessKey: missing Access key ID"),
		},
		"access secret invalid": {
			config: BOAConfiguration{
				Account: "Account Number",
				Region:  "Region",
				Authorizer: &AccessKey{
					AccessKeyID: "accessKeyId",
				},
			},
			expected: fmt.Errorf("AccessKey: missing Access Key secret"),
		},
		"missing authorizer": {
			config: BOAConfiguration{
				Account:    "Account Number",
				Region:     "Region",
				Authorizer: nil,
			},
			expected: fmt.Errorf("BOAConfiguration: missing authorizer"),
		},
		"missing Account": {
			config: BOAConfiguration{
				Account: "",
				Region:  "Region",
				Authorizer: &AccessKey{
					AccessKeyID:     "accessKeyID",
					AccessKeySecret: "accessKeySecret",
				},
			},
			expected: fmt.Errorf("BOAConfiguration: missing account"),
		},
		"missing Region": {
			config: BOAConfiguration{
				Account: "Account",
				Authorizer: &AccessKey{
					AccessKeyID:     "accessKeyID",
					AccessKeySecret: "accessKeySecret",
				},
			},
			expected: fmt.Errorf("BOAConfiguration: missing region"),
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

func TestBOAConfiguration_Equals(t *testing.T) {
	testCases := map[string]struct {
		left     BOAConfiguration
		right    cloud.Config
		expected bool
	}{
		"matching config": {
			left: BOAConfiguration{
				Region:  "region",
				Account: "account",
				Authorizer: &AccessKey{
					AccessKeyID:     "id",
					AccessKeySecret: "secret",
				},
			},
			right: &BOAConfiguration{
				Region:  "region",
				Account: "account",
				Authorizer: &AccessKey{
					AccessKeyID:     "id",
					AccessKeySecret: "secret",
				},
			},
			expected: true,
		},
		"different Authorizer": {
			left: BOAConfiguration{
				Region:  "region",
				Account: "account",
				Authorizer: &AccessKey{
					AccessKeyID:     "id",
					AccessKeySecret: "secret",
				},
			},
			right: &BOAConfiguration{
				Region:  "region",
				Account: "account",
				Authorizer: &AccessKey{
					AccessKeyID:     "id2",
					AccessKeySecret: "secret2",
				},
			},
			expected: false,
		},
		"missing both Authorizer": {
			left: BOAConfiguration{
				Region:     "region",
				Account:    "account",
				Authorizer: nil,
			},
			right: &BOAConfiguration{
				Region:     "region",
				Account:    "account",
				Authorizer: nil,
			},
			expected: true,
		},
		"missing left Authorizer": {
			left: BOAConfiguration{
				Region:     "region",
				Account:    "account",
				Authorizer: nil,
			},
			right: &BOAConfiguration{
				Region:  "region",
				Account: "account",
				Authorizer: &AccessKey{
					AccessKeyID:     "id",
					AccessKeySecret: "secret",
				},
			},
			expected: false,
		},
		"missing right Authorizer": {
			left: BOAConfiguration{
				Region:  "region",
				Account: "account",
				Authorizer: &AccessKey{
					AccessKeyID:     "id",
					AccessKeySecret: "secret",
				},
			},
			right: &BOAConfiguration{
				Region:     "region",
				Account:    "account",
				Authorizer: nil,
			},
			expected: false,
		},
		"different region": {
			left: BOAConfiguration{
				Region:  "region",
				Account: "account",
				Authorizer: &AccessKey{
					AccessKeyID:     "id",
					AccessKeySecret: "secret",
				},
			},
			right: &BOAConfiguration{
				Region:  "region2",
				Account: "account",
				Authorizer: &AccessKey{
					AccessKeyID:     "id",
					AccessKeySecret: "secret",
				},
			},
			expected: false,
		},
		"different account": {
			left: BOAConfiguration{
				Region:  "region",
				Account: "account",
				Authorizer: &AccessKey{
					AccessKeyID:     "id",
					AccessKeySecret: "secret",
				},
			},
			right: &BOAConfiguration{
				Region:  "region",
				Account: "account2",
				Authorizer: &AccessKey{
					AccessKeyID:     "id",
					AccessKeySecret: "secret",
				},
			},
			expected: false,
		},
		"different config": {
			left: BOAConfiguration{
				Region:  "region",
				Account: "account",
				Authorizer: &AccessKey{
					AccessKeyID:     "id",
					AccessKeySecret: "secret",
				},
			},
			right: &AccessKey{
				AccessKeyID:     "id",
				AccessKeySecret: "secret",
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

func TestBOAConfiguration_JSON(t *testing.T) {
	testCases := map[string]struct {
		config BOAConfiguration
	}{
		"Empty Config": {
			config: BOAConfiguration{},
		},
		"AccessKey": {
			config: BOAConfiguration{
				Region:  "region",
				Account: "account",
				Authorizer: &AccessKey{
					AccessKeyID:     "id",
					AccessKeySecret: "secret",
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
			unmarshalledConfig := &BOAConfiguration{}
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

func TestBOAConfiguration_Sanitize(t *testing.T) {
	cfg := BOAConfiguration{
		Account: "account",
		Region:  "region",
		Authorizer: &AccessKey{
			AccessKeyID:     "id",
			AccessKeySecret: "secret",
		},
	}
	sanitized := cfg.Sanitize()
	sanitizedCfg, ok := sanitized.(*BOAConfiguration)
	if !ok {
		t.Fatal("Sanitize did not return *BOAConfiguration")
	}
	if sanitizedCfg.Authorizer != nil {
		if ak, ok := sanitizedCfg.Authorizer.(*AccessKey); ok {
			if ak.AccessKeyID != "id" {
				t.Errorf("Sanitize should not change AccessKeyID: got %q, want %q", ak.AccessKeyID, "id")
			}
			if ak.AccessKeySecret != "REDACTED" {
				t.Errorf("Sanitize should redact AccessKeySecret: got %q, want %q", ak.AccessKeySecret, "REDACTED")
			}
		}
	}
}

func TestBOAConfiguration_Key(t *testing.T) {
	cfg := BOAConfiguration{
		Account: "acc",
		Region:  "reg",
	}
	key := cfg.Key()
	if key == "" {
		t.Error("Key() returned empty string")
	}
}

func TestBOAConfiguration_Provider(t *testing.T) {
	cfg := BOAConfiguration{}
	provider := cfg.Provider()
	if provider != opencost.AlibabaProvider {
		t.Errorf("Provider() = %v, want %v", provider, opencost.AlibabaProvider)
	}
}

func TestBOAConfiguration_UnmarshalJSON_ExtraCases(t *testing.T) {
	// Already tested in TestBOAConfiguration_JSON, but let's add a negative test
	badJSON := []byte(`{"Region": "r", "Account": "a", "Authorizer": {"Type": "Unknown"}}`)
	var cfg BOAConfiguration
	err := cfg.UnmarshalJSON(badJSON)
	if err == nil {
		t.Error("UnmarshalJSON should fail for unknown authorizer type")
	}
}

func TestConvertAlibabaInfoToConfig(t *testing.T) {
	info := AlibabaInfo{
		AlibabaAccountID:        "acc",
		AlibabaClusterRegion:    "reg",
		AlibabaServiceKeyName:   "id",
		AlibabaServiceKeySecret: "secret",
	}
	cfg := ConvertAlibabaInfoToConfig(info)
	boaCfg, ok := cfg.(*BOAConfiguration)
	if !ok {
		t.Fatal("ConvertAlibabaInfoToConfig did not return *BOAConfiguration")
	}
	if boaCfg.Account != info.AlibabaAccountID || boaCfg.Region != info.AlibabaClusterRegion {
		t.Errorf("ConvertAlibabaInfoToConfig did not copy fields correctly")
	}
	ak, ok := boaCfg.Authorizer.(*AccessKey)
	if !ok || ak.AccessKeyID != info.AlibabaServiceKeyName || ak.AccessKeySecret != info.AlibabaServiceKeySecret {
		t.Errorf("ConvertAlibabaInfoToConfig did not set AccessKey correctly")
	}
}
