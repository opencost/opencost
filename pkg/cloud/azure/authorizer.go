package azure

import (
	"encoding/json"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/opencost/opencost/pkg/cloud/config"
)

const AccessKeyAuthorizerType = "AzureAccessKey"
const DefaultAzureCredentialHolderAuthorizerType = "DefaultAzureCredentialHolder"

type Authorizer interface {
	config.Authorizer
	GetBlobClient(urlTemplate string) (*azblob.Client, error)
}

// SelectAuthorizerByType is an implementation of AuthorizerSelectorFn and acts as a register for Authorizer types
func SelectAuthorizerByType(typeStr string) (Authorizer, error) {
	switch typeStr {
	case AccessKeyAuthorizerType:
		return &AccessKey{}, nil
	case DefaultAzureCredentialHolderAuthorizerType:
		return &DefaultAzureCredentialHolder{}, nil
	default:
		return nil, fmt.Errorf("azure: provider authorizer type '%s' is not valid", typeStr)
	}
}

type AccessKey struct {
	AccessKey string `json:"accessKey"`
	Account   string `json:"account"`
}

func (ak *AccessKey) MarshalJSON() ([]byte, error) {
	fmap := make(map[string]any, 4)
	fmap[config.AuthorizerTypeProperty] = AccessKeyAuthorizerType
	fmap["accessKey"] = ak.AccessKey
	fmap["account"] = ak.Account
	return json.Marshal(fmap)
}

func (ak *AccessKey) Validate() error {
	if ak.AccessKey == "" {
		return fmt.Errorf("AccessKey: missing access key")
	}
	if ak.Account == "" {
		return fmt.Errorf("AccessKey: missing account")
	}
	return nil
}

func (ak *AccessKey) Equals(config config.Config) bool {
	if config == nil {
		return false
	}
	thatConfig, ok := config.(*AccessKey)
	if !ok {
		return false
	}

	if ak.AccessKey != thatConfig.AccessKey {
		return false
	}
	if ak.Account != thatConfig.Account {
		return false
	}

	return true
}

func (ak *AccessKey) Sanitize() config.Config {
	return &AccessKey{
		AccessKey: config.Redacted,
		Account:   ak.Account,
	}
}

func (ak *AccessKey) GetBlobClient(urlTemplate string) (*azblob.Client, error) {
	// Create a default request pipeline using your storage account name and account key.
	serviceURL := fmt.Sprintf(urlTemplate, ak.Account, "")

	credential, err := azblob.NewSharedKeyCredential(ak.Account, ak.AccessKey)
	if err != nil {
		return nil, err
	}
	client, err := azblob.NewClientWithSharedKeyCredential(serviceURL, credential, nil)
	return client, err
}

type DefaultAzureCredentialHolder struct {
	Account string `json:"account"`
}

func (dac *DefaultAzureCredentialHolder) MarshalJSON() ([]byte, error) {
	fmap := make(map[string]any, 2)
	fmap[config.AuthorizerTypeProperty] = DefaultAzureCredentialHolderAuthorizerType
	fmap["account"] = dac.Account
	return json.Marshal(fmap)
}

func (dac *DefaultAzureCredentialHolder) Validate() error {
	if dac.Account == "" {
		return fmt.Errorf("AccessKey: missing account")
	}
	return nil
}

func (dac *DefaultAzureCredentialHolder) Equals(config config.Config) bool {
	if config == nil {
		return false
	}
	thatConfig, ok := config.(*DefaultAzureCredentialHolder)
	if !ok {
		return false
	}

	if dac.Account != thatConfig.Account {
		return false
	}

	return true
}

func (dac *DefaultAzureCredentialHolder) Sanitize() config.Config {
	return &DefaultAzureCredentialHolder{}
}

func (dac *DefaultAzureCredentialHolder) GetBlobClient(urlTemplate string) (*azblob.Client, error) {

	serviceURL := fmt.Sprintf(urlTemplate, dac.Account, "")
	// Create a default request pipeline using your storage account name and account key.
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, err
	}

	client, err := azblob.NewClient(serviceURL, cred, nil)
	return client, err
}
