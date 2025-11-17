package azure

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/opencost/opencost/core/pkg/storage"
	"github.com/opencost/opencost/pkg/cloud"
)

const SharedKeyAuthorizerType = "AzureAccessKey"
const StorageConnectionStringAuthorizerType = "AzureStorageConnectionString"

var defaultHTTPConfig = storage.HTTPConfig{
	IdleConnTimeout:       90 * time.Second,
	ResponseHeaderTimeout: 2 * time.Minute,
	TLSHandshakeTimeout:   10 * time.Second,
	ExpectContinueTimeout: 1 * time.Second,
	MaxIdleConns:          100,
	MaxIdleConnsPerHost:   100,
	MaxConnsPerHost:       0,
	DisableCompression:    false,
}

// StorageAuthorizer is a service specific Authorizer for Azure Storage, it exists so that we can support existing Shared
// Key configurations while allowing the Authorizer to have a service agnostic api
type StorageAuthorizer interface {
	cloud.Authorizer
	GetBlobClient(serviceURL string) (*azblob.Client, error)
}

// SelectStorageAuthorizerByType is an implementation of AuthorizerSelectorFn and acts as a register for Authorizer types
func SelectStorageAuthorizerByType(typeStr string) (StorageAuthorizer, error) {
	switch typeStr {
	case SharedKeyAuthorizerType:
		return &SharedKeyCredential{}, nil
	case StorageConnectionStringAuthorizerType:
		return &StorageConnectionStringCredential{
			HTTPConfig: defaultHTTPConfig,
		}, nil
	default:
		authorizer, err := SelectAuthorizerByType(typeStr)
		if err != nil {
			return nil, err
		}
		return &AuthorizerHolder{authorizer}, nil
	}
}

// SharedKeyCredential is a StorageAuthorizer with credentials which cannot be used to authorize other services. This
// is a legacy auth method which is not included in azidentity
type SharedKeyCredential struct {
	AccessKey string `json:"accessKey"`
	Account   string `json:"account"`
}

func (skc *SharedKeyCredential) MarshalJSON() ([]byte, error) {
	fmap := make(map[string]any, 3)
	fmap[cloud.AuthorizerTypeProperty] = SharedKeyAuthorizerType
	fmap["accessKey"] = skc.AccessKey
	fmap["account"] = skc.Account
	return json.Marshal(fmap)
}

func (skc *SharedKeyCredential) Validate() error {
	if skc.AccessKey == "" {
		return fmt.Errorf("SharedKeyCredential: missing access key")
	}
	if skc.Account == "" {
		return fmt.Errorf("SharedKeyCredential: missing account")
	}
	return nil
}

func (skc *SharedKeyCredential) Equals(config cloud.Config) bool {
	if config == nil {
		return false
	}
	thatConfig, ok := config.(*SharedKeyCredential)
	if !ok {
		return false
	}

	if skc.AccessKey != thatConfig.AccessKey {
		return false
	}
	if skc.Account != thatConfig.Account {
		return false
	}

	return true
}

func (skc *SharedKeyCredential) Sanitize() cloud.Config {
	return &SharedKeyCredential{
		AccessKey: cloud.Redacted,
		Account:   skc.Account,
	}
}

func (skc *SharedKeyCredential) GetBlobClient(serviceURL string) (*azblob.Client, error) {
	credential, err := azblob.NewSharedKeyCredential(skc.Account, skc.AccessKey)
	if err != nil {
		return nil, err
	}
	client, err := azblob.NewClientWithSharedKeyCredential(serviceURL, credential, nil)
	return client, err
}

// AuthorizerHolder is a StorageAuthorizer implementation that wraps an Authorizer implementation
type AuthorizerHolder struct {
	Authorizer
}

func (ah *AuthorizerHolder) Equals(config cloud.Config) bool {
	if config == nil {
		return false
	}
	that, ok := config.(*AuthorizerHolder)
	if !ok {
		return false
	}

	return ah.Authorizer.Equals(that.Authorizer)
}

func (ah *AuthorizerHolder) Sanitize() cloud.Config {
	return &AuthorizerHolder{Authorizer: ah.Authorizer.Sanitize().(Authorizer)}
}

func (ah *AuthorizerHolder) GetBlobClient(serviceURL string) (*azblob.Client, error) {
	// Create a default request pipeline using your storage account name and account key.
	cred, err := ah.GetCredential()
	if err != nil {
		return nil, fmt.Errorf("error retrieving credentials: %w", err)
	}

	client, err := azblob.NewClient(serviceURL, cred, nil)
	return client, err
}

// UnmarshalJSON passes the contained Authorizer to be unmarshalled into
func (ah *AuthorizerHolder) UnmarshalJSON(b []byte) error {
	return json.Unmarshal(b, ah.Authorizer)
}

type StorageConnectionStringCredential struct {
	StorageConnectionString string             `json:"storageConnectionString"`
	HTTPConfig              storage.HTTPConfig `json:"httpConfig"`
}

func (s *StorageConnectionStringCredential) MarshalJSON() ([]byte, error) {
	fmap := make(map[string]any, 3)
	fmap[cloud.AuthorizerTypeProperty] = StorageConnectionStringAuthorizerType
	fmap["storageConnectionString"] = s.StorageConnectionString
	fmap["httpConfig"] = s.HTTPConfig
	return json.Marshal(fmap)
}

func (s *StorageConnectionStringCredential) Validate() error {
	if s.StorageConnectionString == "" {
		return fmt.Errorf("StorageConnectionStringCredential: missing storage connection string")
	}
	return nil
}

func (s *StorageConnectionStringCredential) Equals(config cloud.Config) bool {
	if config == nil {
		return false
	}

	thatConfig, ok := config.(*StorageConnectionStringCredential)
	if !ok {
		return false
	}

	if s.HTTPConfig != thatConfig.HTTPConfig {
		return false
	}

	if s.StorageConnectionString != thatConfig.StorageConnectionString {
		return false
	}

	return true
}

func (s *StorageConnectionStringCredential) Sanitize() cloud.Config {
	return &StorageConnectionStringCredential{
		StorageConnectionString: cloud.Redacted,
		HTTPConfig:              s.HTTPConfig,
	}
}

func (s *StorageConnectionStringCredential) GetBlobClient(serviceURL string) (*azblob.Client, error) {
	dt, err := s.HTTPConfig.GetHTTPTransport()
	if err != nil {
		return nil, fmt.Errorf("error creating transport: %w", err)
	}
	options := &azblob.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			Transport: &http.Client{Transport: dt},
		},
	}
	client, err := azblob.NewClientFromConnectionString(s.StorageConnectionString, options)
	return client, err
}
