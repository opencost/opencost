package ibm

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/IBM/platform-services-go-sdk/usagereportsv4"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/pkg/cloud"
)

// UsageConfiguration connects OpenCost to the IBM Cloud Usage Reports API
// (account cost and usage / CUR-style billing data).
type UsageConfiguration struct {
	AccountID  string     `json:"accountID"`
	Authorizer Authorizer `json:"authorizer"`
}

func (c *UsageConfiguration) Validate() error {
	if c.Authorizer == nil {
		return fmt.Errorf("UsageConfiguration: missing Authorizer")
	}
	if err := c.Authorizer.Validate(); err != nil {
		return fmt.Errorf("UsageConfiguration: %s", err)
	}
	if c.AccountID == "" {
		return fmt.Errorf("UsageConfiguration: missing accountID")
	}
	return nil
}

func (c *UsageConfiguration) Equals(config cloud.Config) bool {
	if config == nil {
		return false
	}
	that, ok := config.(*UsageConfiguration)
	if !ok {
		return false
	}
	if c.Authorizer != nil {
		if !c.Authorizer.Equals(that.Authorizer) {
			return false
		}
	} else if that.Authorizer != nil {
		return false
	}
	return c.AccountID == that.AccountID
}

func (c *UsageConfiguration) Sanitize() cloud.Config {
	var auth Authorizer
	if c.Authorizer != nil {
		auth = c.Authorizer.Sanitize().(Authorizer)
	}
	return &UsageConfiguration{
		AccountID:  c.AccountID,
		Authorizer: auth,
	}
}

// Key identifies the integration for storage paths. Slash is replaced so an
// accidental "a/<hex>" account id cannot create an extra path segment.
func (c *UsageConfiguration) Key() string {
	return strings.ReplaceAll(c.AccountID, "/", "-")
}

func (c *UsageConfiguration) Provider() string {
	return opencost.IBMProvider
}

func (c *UsageConfiguration) GetUsageReportsClient() (*usagereportsv4.UsageReportsV4, error) {
	if c.Authorizer == nil {
		return nil, fmt.Errorf("missing authorizer")
	}
	authenticator, err := c.Authorizer.CreateAuthenticator()
	if err != nil {
		return nil, fmt.Errorf("creating IAM authenticator: %w", err)
	}
	client, err := usagereportsv4.NewUsageReportsV4(&usagereportsv4.UsageReportsV4Options{
		Authenticator: authenticator,
	})
	if err != nil {
		return nil, fmt.Errorf("creating usage reports client: %w", err)
	}
	return client, nil
}

func (c *UsageConfiguration) UnmarshalJSON(b []byte) error {
	var f any
	if err := json.Unmarshal(b, &f); err != nil {
		return err
	}
	fmap, ok := f.(map[string]any)
	if !ok {
		return fmt.Errorf("UsageConfiguration: UnmarshalJSON: expected object")
	}

	accountID, err := cloud.GetInterfaceValue[string](fmap, "accountID")
	if err != nil {
		return fmt.Errorf("UsageConfiguration: UnmarshalJSON: %w", err)
	}
	c.AccountID = accountID

	authAny, ok := fmap["authorizer"]
	if !ok {
		return fmt.Errorf("UsageConfiguration: UnmarshalJSON: missing authorizer")
	}
	authorizer, err := cloud.AuthorizerFromInterface(authAny, SelectAuthorizerByType)
	if err != nil {
		return fmt.Errorf("UsageConfiguration: UnmarshalJSON: %w", err)
	}
	c.Authorizer = authorizer
	return nil
}
