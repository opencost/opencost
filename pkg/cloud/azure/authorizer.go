package azure

import (
	"encoding/json"
	"fmt"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/opencost/opencost/pkg/cloud/config"
)

const AccessKeyAuthorizerType = "AzureAccessKey"

type Authorizer interface {
	config.Authorizer
	GetBlobCredentials() (azblob.Credential, error)
}

// SelectAuthorizerByType is an implementation of AuthorizerSelectorFn and acts as a register for Authorizer types
func SelectAuthorizerByType(typeStr string) (Authorizer, error) {
	switch typeStr {
	case AccessKeyAuthorizerType:
		return &AccessKey{}, nil
	default:
		return nil, fmt.Errorf("azure: provider authorizer type '%s' is not valid", typeStr)
	}
}

type AccessKey struct {
	AccessKey string `json:"accessKey"`
	Account   string `json:"account"`
}

func (ak *AccessKey) MarshalJSON() ([]byte, error) {
	fmap := make(map[string]any, 3)
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

func (ak *AccessKey) GetBlobCredentials() (azblob.Credential, error) {
	// Create a default request pipeline using your storage account name and account key.
	return azblob.NewSharedKeyCredential(ak.Account, ak.AccessKey)
}
