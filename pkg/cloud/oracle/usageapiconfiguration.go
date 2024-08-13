package oracle

import (
	"encoding/json"
	"fmt"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/pkg/cloud"
	"github.com/oracle/oci-go-sdk/v65/usageapi"
)

type UsageApiConfiguration struct {
	Authorizer Authorizer `json:"authorizer"`
}

func (uac *UsageApiConfiguration) Validate() error {
	// Validate Authorizer
	if uac.Authorizer == nil {
		return fmt.Errorf("UsageApiConfiguration: missing Authorizer")
	}

	err := uac.Authorizer.Validate()
	if err != nil {
		return fmt.Errorf("UsageApiConfiguration: %s", err)
	}

	return nil
}

func (uac *UsageApiConfiguration) Equals(config cloud.Config) bool {
	if config == nil {
		return false
	}
	thatConfig, ok := config.(*UsageApiConfiguration)
	if !ok {
		return false
	}

	if uac.Authorizer != nil {
		if !uac.Authorizer.Equals(thatConfig.Authorizer) {
			return false
		}
	} else {
		if thatConfig.Authorizer != nil {
			return false
		}
	}

	return true
}

func (uac *UsageApiConfiguration) Sanitize() cloud.Config {
	return &UsageApiConfiguration{
		Authorizer: uac.Authorizer.Sanitize().(Authorizer),
	}
}

func (uac *UsageApiConfiguration) Key() string {
	//todo: dont do this
	return fmt.Sprintf("%s", uac.Authorizer)
}

func (uac *UsageApiConfiguration) Provider() string {
	return opencost.OracleProvider
}

func (uac *UsageApiConfiguration) GetUsageApiClient() (*usageapi.UsageapiClient, error) {
	configProvider, err := uac.Authorizer.CreateOCIConfig()
	if err != nil {
		return nil, err
	}
	client, err := usageapi.NewUsageapiClientWithConfigurationProvider(configProvider)
	return &client, err
}

func (uac *UsageApiConfiguration) UnmarshalJSON(b []byte) error {
	var f interface{}
	err := json.Unmarshal(b, &f)
	if err != nil {
		return err
	}

	fmap := f.(map[string]interface{})

	authAny, ok := fmap["authorizer"]
	if !ok {
		return fmt.Errorf("UsageApiConfiguration: UnmarshalJSON: missing authorizer")
	}
	authorizer, err := cloud.AuthorizerFromInterface(authAny, SelectAuthorizerByType)
	if err != nil {
		return fmt.Errorf("UsageApiConfiguration: UnmarshalJSON: %w", err)
	}
	uac.Authorizer = authorizer

	return nil
}
