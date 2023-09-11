package azure

import (
	"fmt"

	"github.com/opencost/opencost/pkg/cloud/config"
	"github.com/opencost/opencost/pkg/util/json"
)

type StorageConfiguration struct {
	SubscriptionID string     `json:"subscriptionID"`
	Account        string     `json:"account"`
	Container      string     `json:"container"`
	Path           string     `json:"path"`
	Cloud          string     `json:"cloud"`
	Authorizer     Authorizer `json:"authorizer"`
}

// Check ensures that all required fields are set, and throws an error if they are not
func (sc *StorageConfiguration) Validate() error {

	if sc.Authorizer == nil {
		return fmt.Errorf("StorageConfiguration: missing authorizer")
	}

	err := sc.Authorizer.Validate()
	if err != nil {
		return err
	}

	if sc.SubscriptionID == "" {
		return fmt.Errorf("StorageConfiguration: missing Subcription ID")
	}

	if sc.Account == "" {
		return fmt.Errorf("StorageConfiguration: missing Account")
	}

	if sc.Container == "" {
		return fmt.Errorf("StorageConfiguration: missing Container")
	}

	return nil
}

func (sc *StorageConfiguration) Equals(config config.Config) bool {
	if config == nil {
		return false
	}
	thatConfig, ok := config.(*StorageConfiguration)
	if !ok {
		return false
	}

	if sc.Authorizer != nil {
		if !sc.Authorizer.Equals(thatConfig.Authorizer) {
			return false
		}
	} else {
		if thatConfig.Authorizer != nil {
			return false
		}
	}

	if sc.SubscriptionID != thatConfig.SubscriptionID {
		return false
	}

	if sc.Account != thatConfig.Account {
		return false
	}

	if sc.Container != thatConfig.Container {
		return false
	}

	if sc.Path != thatConfig.Path {
		return false
	}

	if sc.Cloud != thatConfig.Cloud {
		return false
	}

	return true
}

func (sc *StorageConfiguration) Sanitize() config.Config {
	return &StorageConfiguration{
		SubscriptionID: sc.SubscriptionID,
		Account:        sc.Account,
		Container:      sc.Container,
		Path:           sc.Path,
		Cloud:          sc.Cloud,
		Authorizer:     sc.Authorizer.Sanitize().(Authorizer),
	}
}

func (sc *StorageConfiguration) Key() string {
	key := fmt.Sprintf("%s/%s", sc.SubscriptionID, sc.Container)
	// append container path to key if it exists
	if sc.Path != "" {
		key = fmt.Sprintf("%s/%s", key, sc.Path)
	}
	return key
}

func (sc *StorageConfiguration) UnmarshalJSON(b []byte) error {
	var f interface{}
	err := json.Unmarshal(b, &f)
	if err != nil {
		return err
	}

	fmap := f.(map[string]interface{})

	subscriptionID, err := config.GetInterfaceValue[string](fmap, "subscriptionID")
	if err != nil {
		return fmt.Errorf("StorageConfiguration: UnmarshalJSON: %s", err.Error())
	}
	sc.SubscriptionID = subscriptionID

	account, err := config.GetInterfaceValue[string](fmap, "account")
	if err != nil {
		return fmt.Errorf("StorageConfiguration: UnmarshalJSON: %s", err.Error())
	}
	sc.Account = account

	container, err := config.GetInterfaceValue[string](fmap, "container")
	if err != nil {
		return fmt.Errorf("StorageConfiguration: UnmarshalJSON: %s", err.Error())
	}
	sc.Container = container

	path, err := config.GetInterfaceValue[string](fmap, "path")
	if err != nil {
		return fmt.Errorf("StorageConfiguration: UnmarshalJSON: %s", err.Error())
	}
	sc.Path = path

	cloud, err := config.GetInterfaceValue[string](fmap, "cloud")
	if err != nil {
		return fmt.Errorf("StorageConfiguration: UnmarshalJSON: %s", err.Error())
	}
	sc.Cloud = cloud

	authAny, ok := fmap["authorizer"]
	if !ok {
		return fmt.Errorf("StorageConfiguration: UnmarshalJSON: missing authorizer")
	}
	authorizer, err := config.AuthorizerFromInterface(authAny, SelectAuthorizerByType)
	if err != nil {
		return fmt.Errorf("StorageConfiguration: UnmarshalJSON: %s", err.Error())
	}
	sc.Authorizer = authorizer

	return nil
}

func ConvertAzureStorageConfigToConfig(asc AzureStorageConfig) config.KeyedConfig {
	if asc.IsEmpty() {
		return nil
	}

	var authorizer Authorizer
	authorizer = &AccessKey{
		AccessKey: asc.AccessKey,
		Account:   asc.AccountName,
	}

	return &StorageConfiguration{
		SubscriptionID: asc.SubscriptionId,
		Account:        asc.AccountName,
		Container:      asc.ContainerName,
		Path:           asc.ContainerPath,
		Cloud:          asc.AzureCloud,
		Authorizer:     authorizer,
	}
}
