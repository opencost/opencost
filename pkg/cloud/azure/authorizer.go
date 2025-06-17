package azure

import (
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/pkg/cloud"
)

const (
	DefaultCredentialAuthorizerType = "AzureDefaultCredential"
	ClientSecretCredentialType      = "AzureClientSecretCredential"
)

// Authorizer configs provide credentials from azidentity to connect to Azure services.
type Authorizer interface {
	cloud.Authorizer
	GetCredential() (azcore.TokenCredential, error)
}

// SelectAuthorizerByType is an implementation of AuthorizerSelectorFn and acts as a register for Authorizer types
func SelectAuthorizerByType(typeStr string) (Authorizer, error) {
	switch typeStr {
	case DefaultCredentialAuthorizerType:
		return &DefaultAzureCredentialHolder{}, nil
	case ClientSecretCredentialType:
		return &ClientSecretCredential{}, nil
	default:
		return nil, fmt.Errorf("azure: provider authorizer type '%s' is not valid", typeStr)
	}
}

type DefaultAzureCredentialHolder struct{}

func (dac *DefaultAzureCredentialHolder) MarshalJSON() ([]byte, error) {
	fmap := make(map[string]any, 1)
	fmap[cloud.AuthorizerTypeProperty] = DefaultCredentialAuthorizerType

	return json.Marshal(fmap)
}

func (dac *DefaultAzureCredentialHolder) Validate() error {
	return nil
}

func (dac *DefaultAzureCredentialHolder) Equals(config cloud.Config) bool {
	if config == nil {
		return false
	}
	_, ok := config.(*DefaultAzureCredentialHolder)
	if !ok {
		return false
	}
	return true
}

func (dac *DefaultAzureCredentialHolder) Sanitize() cloud.Config {
	return &DefaultAzureCredentialHolder{}
}

func (dac *DefaultAzureCredentialHolder) GetCredential() (azcore.TokenCredential, error) {
	return azidentity.NewDefaultAzureCredential(nil)
}

type ClientSecretCredential struct {
	TenantID     string `json:"tenantID"`
	ClientID     string `json:"clientID"`
	ClientSecret string `json:"clientSecret"`
}

func (csc *ClientSecretCredential) Validate() error {
	if csc.TenantID == "" {
		return fmt.Errorf("ClientSecretCredential: missing Tenant ID")
	}
	if csc.ClientID == "" {
		return fmt.Errorf("ClientSecretCredential: missing Client ID")
	}
	if csc.ClientSecret == "" {
		return fmt.Errorf("ClientSecretCredential: missing Client Secret")
	}
	return nil
}

func (csc *ClientSecretCredential) Sanitize() cloud.Config {
	return &ClientSecretCredential{
		TenantID:     csc.TenantID,
		ClientID:     csc.ClientID,
		ClientSecret: cloud.Redacted,
	}
}

func (csc *ClientSecretCredential) Equals(config cloud.Config) bool {
	if config == nil {
		return false
	}
	thatConfig, ok := config.(*ClientSecretCredential)
	if !ok {
		return false
	}

	if csc.TenantID != thatConfig.TenantID {
		return false
	}
	if csc.ClientID != thatConfig.ClientID {
		return false
	}
	if csc.ClientSecret != thatConfig.ClientSecret {
		return false
	}
	return true
}

func (csc *ClientSecretCredential) MarshalJSON() ([]byte, error) {
	fmap := make(map[string]any, 1)
	fmap[cloud.AuthorizerTypeProperty] = ClientSecretCredentialType
	fmap["tenantID"] = csc.TenantID
	fmap["clientID"] = csc.ClientID
	fmap["clientSecret"] = csc.ClientSecret
	return json.Marshal(fmap)
}

func (csc *ClientSecretCredential) GetCredential() (azcore.TokenCredential, error) {
	cred, err := azidentity.NewClientSecretCredential(csc.TenantID, csc.ClientID, csc.ClientSecret, nil)
	if err != nil {
		return nil, fmt.Errorf("ClientSecretCredential: failed to retrieve credentials: %w", err)
	}
	return cred, nil
}
