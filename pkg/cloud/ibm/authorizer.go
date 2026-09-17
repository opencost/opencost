package ibm

import (
	"encoding/json"
	"fmt"

	"github.com/IBM/go-sdk-core/v5/core"
	"github.com/opencost/opencost/pkg/cloud"
)

const APIKeyAuthorizerType = "IBMAPIKey"

// Authorizer creates IBM Cloud SDK authenticators for Usage Reports clients.
type Authorizer interface {
	cloud.Authorizer
	CreateAuthenticator() (core.Authenticator, error)
}

// SelectAuthorizerByType registers supported IBM authorizer types.
func SelectAuthorizerByType(typeStr string) (Authorizer, error) {
	switch typeStr {
	case APIKeyAuthorizerType:
		return &APIKey{}, nil
	default:
		return nil, fmt.Errorf("IBM: provider authorizer type '%s' is not valid", typeStr)
	}
}

// APIKey authenticates to IBM Cloud IAM with an API key.
// The key may belong to a user or service ID with billing.usage-report.read.
type APIKey struct {
	Key string `json:"apiKey"`
}

func (a *APIKey) MarshalJSON() ([]byte, error) {
	fmap := map[string]any{
		cloud.AuthorizerTypeProperty: APIKeyAuthorizerType,
		"apiKey":                     a.Key,
	}
	return json.Marshal(fmap)
}

func (a *APIKey) Validate() error {
	if a.Key == "" {
		return fmt.Errorf("APIKey: missing apiKey")
	}
	return nil
}

func (a *APIKey) Equals(config cloud.Config) bool {
	if config == nil {
		return false
	}
	that, ok := config.(*APIKey)
	if !ok {
		return false
	}
	return a.Key == that.Key
}

func (a *APIKey) Sanitize() cloud.Config {
	return &APIKey{Key: cloud.Redacted}
}

func (a *APIKey) CreateAuthenticator() (core.Authenticator, error) {
	return core.NewIamAuthenticatorBuilder().SetApiKey(a.Key).Build()
}
