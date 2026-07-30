package huawei

import (
	"encoding/json"
	"fmt"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/pkg/cloud"
)

// CostConfiguration holds the configuration needed to query historical billing
// costs from the Huawei Cloud BSS "cost-analysed-bills" API (see costintegration.go).
// Credentials (HUAWEICLOUD_ACCESS_KEY_ID, HUAWEICLOUD_SECRET_ACCESS_KEY,
// HUAWEICLOUD_DOMAIN_ID) are intentionally not part of this struct -- like the rest
// of pkg/cloud/huawei, they are read from the environment, so they never need to be
// written to disk in a config file or serialized by the admin config-export API.
type CostConfiguration struct {
	ProjectID string `json:"projectID"`
	Region    string `json:"region"`
}

func (c *CostConfiguration) Validate() error {
	if c.ProjectID == "" {
		return fmt.Errorf("CostConfiguration: missing projectID")
	}
	if c.Region == "" {
		return fmt.Errorf("CostConfiguration: missing region")
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
	return c.ProjectID == thatConfig.ProjectID && c.Region == thatConfig.Region
}

// Sanitize returns a copy of the config safe to serialize/display. There is
// nothing secret to redact here since credentials are not stored in this struct.
func (c *CostConfiguration) Sanitize() cloud.Config {
	return &CostConfiguration{
		ProjectID: c.ProjectID,
		Region:    c.Region,
	}
}

func (c *CostConfiguration) Key() string {
	return c.ProjectID
}

func (c *CostConfiguration) Provider() string {
	return opencost.HuaweiProvider
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

	projectID, err := cloud.GetInterfaceValue[string](fmap, "projectID")
	if err != nil {
		return fmt.Errorf("CostConfiguration: UnmarshalJSON: %w", err)
	}
	if projectID == "" {
		// cloud-integration.json ships with an empty projectID when no static
		// HUAWEICLOUD_PROJECT_ID/values.yaml cloud.projectId was provisioned. Fall
		// back to the instance metadata service (same mechanism
		// huaweiGlobalCredentials uses for AK/SK), so a node with an IAM agency
		// attached needs no project ID configured anywhere either.
		if metaProjectID, metaErr := huaweiProjectIDFromMetadata(); metaErr == nil {
			projectID = metaProjectID
		}
	}
	c.ProjectID = projectID

	region, err := cloud.GetInterfaceValue[string](fmap, "region")
	if err != nil {
		return fmt.Errorf("CostConfiguration: UnmarshalJSON: %w", err)
	}
	c.Region = region

	return nil
}
