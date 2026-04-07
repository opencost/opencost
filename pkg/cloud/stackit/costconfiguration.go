package stackit

import (
	"encoding/json"
	"fmt"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/pkg/cloud"
)

// CostConfiguration holds the configuration needed to connect to STACKIT Cost API
type CostConfiguration struct {
	CustomerAccountID     string `json:"customerAccountId"`
	ProjectID             string `json:"projectId"`
	ServiceAccountKeyPath string `json:"serviceAccountKeyPath,omitempty"`
}

func (c *CostConfiguration) Validate() error {
	if c.CustomerAccountID == "" {
		return fmt.Errorf("CostConfiguration: missing customerAccountId")
	}
	if c.ProjectID == "" {
		return fmt.Errorf("CostConfiguration: missing projectId")
	}
	return nil
}

func (c *CostConfiguration) Equals(config cloud.Config) bool {
	if config == nil {
		return false
	}
	thatConfig, ok := config.(*CostConfiguration)
	if !ok {
		return false
	}

	return c.CustomerAccountID == thatConfig.CustomerAccountID &&
		c.ProjectID == thatConfig.ProjectID &&
		c.ServiceAccountKeyPath == thatConfig.ServiceAccountKeyPath
}

func (c *CostConfiguration) Sanitize() cloud.Config {
	return &CostConfiguration{
		CustomerAccountID:     c.CustomerAccountID,
		ProjectID:             c.ProjectID,
		ServiceAccountKeyPath: cloud.Redacted,
	}
}

func (c *CostConfiguration) Key() string {
	return c.CustomerAccountID + "/" + c.ProjectID
}

func (c *CostConfiguration) Provider() string {
	return opencost.STACKITProvider
}

func (c *CostConfiguration) UnmarshalJSON(b []byte) error {
	var f interface{}
	err := json.Unmarshal(b, &f)
	if err != nil {
		return err
	}

	fmap, ok := f.(map[string]interface{})
	if !ok {
		return fmt.Errorf("CostConfiguration: UnmarshalJSON: expected object")
	}

	customerAccountID, err := cloud.GetInterfaceValue[string](fmap, "customerAccountId")
	if err != nil {
		return fmt.Errorf("CostConfiguration: UnmarshalJSON: %w", err)
	}
	c.CustomerAccountID = customerAccountID

	projectID, err := cloud.GetInterfaceValue[string](fmap, "projectId")
	if err != nil {
		return fmt.Errorf("CostConfiguration: UnmarshalJSON: %w", err)
	}
	c.ProjectID = projectID

	if saKeyPath, ok := fmap["serviceAccountKeyPath"]; ok {
		if s, ok := saKeyPath.(string); ok {
			c.ServiceAccountKeyPath = s
		}
	}

	return nil
}
