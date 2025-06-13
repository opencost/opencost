package azure

import (
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/pkg/cloud"
)

const DefaultCredentialAuthorizerType = "AzureDefaultCredential"
const ClientSecretCredentialType = "AzureClientSecretCredential"

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

func (c *ClientSecretCredential) Validate() error {
	if c.TenantID == "" {
		return fmt.Errorf("ClientSecretCredential: missing Tenant ID")
	}
	if c.ClientID == "" {
		return fmt.Errorf("ClientSecretCredential: missing Client ID")
	}
	if c.ClientSecret == "" {
		return fmt.Errorf("ClientSecretCredential: missing Client Secret")
	}
	return nil
}

func (c *ClientSecretCredential) Sanitize() cloud.Config {
	return &ClientSecretCredential{
		TenantID:     c.TenantID,
		ClientID:     c.ClientID,
		ClientSecret: cloud.Redacted,
	}
}

func (c *ClientSecretCredential) Equals(config cloud.Config) bool {
	if config == nil {
		return false
	}
	thatConfig, ok := config.(*ClientSecretCredential)
	if !ok {
		return false
	}

	if c.TenantID != thatConfig.TenantID {
		return false
	}
	if c.ClientID != thatConfig.ClientID {
		return false
	}
	if c.ClientSecret != thatConfig.ClientSecret {
		return false
	}
	return true
}

func (c *ClientSecretCredential) MarshalJSON() ([]byte, error) {
	fmap := make(map[string]any, 1)
	fmap[cloud.AuthorizerTypeProperty] = ClientSecretCredentialType
	fmap["tenantID"] = c.TenantID
	fmap["clientID"] = c.ClientID
	fmap["clientSecret"] = c.ClientSecret
	return json.Marshal(fmap)
}

func (c *ClientSecretCredential) GetCredential() (azcore.TokenCredential, error) {
	cred, err := azidentity.NewClientSecretCredential(c.TenantID, c.ClientID, c.ClientSecret, nil)
	if err != nil {
		return nil, fmt.Errorf("ClientSecretCredential: failed to retrieve credentials: %w", err)
	}
	return cred, nil
}
