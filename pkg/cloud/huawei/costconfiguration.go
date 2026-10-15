package huawei

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"

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

	// EnterpriseProjects names the account's Enterprise Projects and the
	// platform each one holds. Every billed resource is labeled with its
	// Enterprise Project whether or not it is listed here; listing one only
	// adds its name and platform to the labels. Optional.
	EnterpriseProjects []EnterpriseProject `json:"enterpriseProjects,omitempty"`

	// PriceFactors scale the net cost of matching resources, for prices agreed
	// outside the bill (e.g. a reseller price that is a fixed fraction of the
	// list price). The list cost is left as billed. Optional.
	PriceFactors []PriceFactor `json:"priceFactors,omitempty"`
}

// EnterpriseProject describes one Huawei Cloud Enterprise Project.
type EnterpriseProject struct {
	// ID is the Enterprise Project ID as BSS reports it ("0" for default).
	ID string `json:"id"`
	// Name is a display name; the ID stands in when it is empty.
	Name string `json:"name,omitempty"`
	// Platform is the platform the Enterprise Project holds, e.g.
	// "web-prod". Several Enterprise Projects may share a platform.
	Platform string `json:"platform,omitempty"`
}

// PriceFactor scales the net cost of the resources it matches. Service and
// ResourceType are Huawei Cloud codes, with or without their
// "hws.service.type." / "hws.resource.type." prefixes; an empty one matches
// anything. The first matching factor applies.
type PriceFactor struct {
	Service      string  `json:"service,omitempty"`
	ResourceType string  `json:"resourceType,omitempty"`
	Factor       float64 `json:"factor"`
}

// Validate does not require a projectID: when it is absent, it is resolved
// from the instance metadata service at integration time (see resolveProjectID),
// so a node with an IAM agency attached needs none configured.
func (c *CostConfiguration) Validate() error {
	if c.Region == "" {
		return fmt.Errorf("CostConfiguration: missing region")
	}
	for i, ep := range c.EnterpriseProjects {
		if ep.ID == "" {
			return fmt.Errorf("CostConfiguration: enterpriseProjects[%d]: missing id", i)
		}
	}
	for i, pf := range c.PriceFactors {
		if pf.Factor <= 0 {
			return fmt.Errorf("CostConfiguration: priceFactors[%d]: factor must be positive, got %v", i, pf.Factor)
		}
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
	return c.ProjectID == thatConfig.ProjectID &&
		c.Region == thatConfig.Region &&
		slices.Equal(c.EnterpriseProjects, thatConfig.EnterpriseProjects) &&
		slices.Equal(c.PriceFactors, thatConfig.PriceFactors)
}

// Sanitize returns a copy of the config safe to serialize/display. There is
// nothing secret to redact here since credentials are not stored in this struct.
func (c *CostConfiguration) Sanitize() cloud.Config {
	return &CostConfiguration{
		ProjectID:          c.ProjectID,
		Region:             c.Region,
		EnterpriseProjects: slices.Clone(c.EnterpriseProjects),
		PriceFactors:       slices.Clone(c.PriceFactors),
	}
}

// Key identifies this integration. The project ID is the natural identifier,
// but it is optional (see Validate), so the region stands in when it is absent
// -- an account has one Huawei integration per region either way.
func (c *CostConfiguration) Key() string {
	if c.ProjectID != "" {
		return c.ProjectID
	}
	return c.Region
}

// resolveProjectID returns the configured project ID, falling back to the
// instance metadata service for nodes that have an IAM agency attached but no
// project ID provisioned. The lookup is deliberately here rather than in
// UnmarshalJSON: unmarshalling a config must not touch the network. Results are
// cached, see projectIDCacheTTL.
func (c *CostConfiguration) resolveProjectID() (string, error) {
	if c.ProjectID != "" {
		return c.ProjectID, nil
	}
	projectID, err := huaweiProjectIDFromMetadata()
	if err != nil {
		return "", fmt.Errorf("CostConfiguration: no projectID configured and IAM agency metadata lookup failed: %w", err)
	}
	return projectID, nil
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
	// An empty projectID is allowed: it is resolved from instance metadata at
	// integration time instead (see resolveProjectID).
	c.ProjectID = projectID

	region, err := cloud.GetInterfaceValue[string](fmap, "region")
	if err != nil {
		return fmt.Errorf("CostConfiguration: UnmarshalJSON: %w", err)
	}
	c.Region = region

	// The optional lists are plain data; let encoding/json decode them.
	var optional struct {
		EnterpriseProjects []EnterpriseProject `json:"enterpriseProjects"`
		PriceFactors       []PriceFactor       `json:"priceFactors"`
	}
	if err := json.Unmarshal(b, &optional); err != nil {
		return fmt.Errorf("CostConfiguration: UnmarshalJSON: %w", err)
	}
	c.EnterpriseProjects = optional.EnterpriseProjects
	c.PriceFactors = optional.PriceFactors

	return nil
}

// enterpriseProject returns the configured description of an Enterprise
// Project ID, or one carrying just the ID when it is not configured. The
// default Enterprise Project is named "default" unless configured otherwise.
func (c *CostConfiguration) enterpriseProject(id string) EnterpriseProject {
	for _, ep := range c.EnterpriseProjects {
		if ep.ID == id {
			if ep.Name == "" {
				ep.Name = id
			}
			return ep
		}
	}
	if id == defaultEnterpriseProjectID {
		return EnterpriseProject{ID: id, Name: "default"}
	}
	return EnterpriseProject{ID: id, Name: id}
}

// priceFactor returns the factor that applies to a resource of the given
// service and resource type codes, or 1 when none does.
func (c *CostConfiguration) priceFactor(serviceCode, resourceTypeCode string) float64 {
	for _, pf := range c.PriceFactors {
		if codeMatches(pf.Service, serviceCode, serviceTypeCodePrefix) &&
			codeMatches(pf.ResourceType, resourceTypeCode, huaweiResourceTypeCodePrefix) {
			return pf.Factor
		}
	}
	return 1
}

// codeMatches compares a configured code with a billed one, ignoring the
// "hws.<kind>.type." prefix on either side. An empty pattern matches anything.
func codeMatches(pattern, code, prefix string) bool {
	if pattern == "" {
		return true
	}
	trim := func(s string) string {
		return strings.TrimPrefix(strings.ToLower(strings.TrimSpace(s)), prefix)
	}
	return trim(pattern) == trim(code)
}
