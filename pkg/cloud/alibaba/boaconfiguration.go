package alibaba

import (
	"fmt"

	"github.com/opencost/opencost/pkg/cloud/config"
	"github.com/opencost/opencost/pkg/util/json"
)

// BOAConfiguration is the BSS open API configuration for Alibaba's Billing information
type BOAConfiguration struct {
	Account    string     `json:"account"`
	Region     string     `json:"region"`
	Authorizer Authorizer `json:"authorizer"`
}

func (bc *BOAConfiguration) Validate() error {
	// Validate Authorizer
	if bc.Authorizer == nil {
		return fmt.Errorf("BOAConfiguration: missing authorizer")
	}

	err := bc.Authorizer.Validate()
	if err != nil {
		return err
	}

	// Validate base properties
	if bc.Region == "" {
		return fmt.Errorf("BOAConfiguration: missing region")
	}

	if bc.Account == "" {
		return fmt.Errorf("BOAConfiguration: missing account")
	}
	return nil
}

func (bc *BOAConfiguration) Equals(config config.Config) bool {
	if config == nil {
		return false
	}
	thatConfig, ok := config.(*BOAConfiguration)
	if !ok {
		return false
	}

	if bc.Authorizer != nil {
		if !bc.Authorizer.Equals(thatConfig.Authorizer) {
			return false
		}
	} else {
		if thatConfig.Authorizer != nil {
			return false
		}
	}

	if bc.Account != thatConfig.Account {
		return false
	}

	if bc.Region != thatConfig.Region {
		return false
	}
	return true
}

func (bc *BOAConfiguration) Sanitize() config.Config {
	return &BOAConfiguration{
		Account:    bc.Account,
		Region:     bc.Region,
		Authorizer: bc.Authorizer.Sanitize().(Authorizer),
	}
}

func (bc *BOAConfiguration) Key() string {
	return fmt.Sprintf("%s/%s", bc.Account, bc.Region)
}

func (bc *BOAConfiguration) UnmarshalJSON(b []byte) error {
	var f interface{}
	err := json.Unmarshal(b, &f)
	if err != nil {
		return err
	}

	fmap := f.(map[string]interface{})

	account, err := config.GetInterfaceValue[string](fmap, "account")
	if err != nil {
		return fmt.Errorf("BOAConfiguration: UnmarshalJSON: %s", err.Error())
	}
	bc.Account = account

	region, err := config.GetInterfaceValue[string](fmap, "region")
	if err != nil {
		return fmt.Errorf("BOAConfiguration: UnmarshalJSON: %s", err.Error())
	}
	bc.Region = region

	authAny, ok := fmap["authorizer"]
	if !ok {
		return fmt.Errorf("BOAConfiguration: UnmarshalJSON: missing authorizer")
	}
	authorizer, err := config.AuthorizerFromInterface(authAny, SelectAuthorizerByType)
	if err != nil {
		return fmt.Errorf("BOAConfiguration: UnmarshalJSON: %s", err.Error())
	}
	bc.Authorizer = authorizer

	return nil
}

func ConvertAlibabaInfoToConfig(acc AlibabaInfo) config.KeyedConfig {
	if acc.IsEmpty() {
		return nil
	}
	var configurer Authorizer

	configurer = &AccessKey{
		AccessKeyID:     acc.AlibabaServiceKeyName,
		AccessKeySecret: acc.AlibabaServiceKeySecret,
	}

	return &BOAConfiguration{
		Account:    acc.AlibabaAccountID,
		Region:     acc.AlibabaClusterRegion,
		Authorizer: configurer,
	}
}
