package aws

import (
	"fmt"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/pkg/cloud"
)

// AthenaConfiguration
type AthenaConfiguration struct {
	Bucket     string     `json:"bucket"`
	Region     string     `json:"region"`
	Database   string     `json:"database"`
	Catalog    string     `json:"catalog"`
	Table      string     `json:"table"`
	Workgroup  string     `json:"workgroup"`
	Account    string     `json:"account"`
	Authorizer Authorizer `json:"authorizer"`
	CURVersion string     `json:"curVersion,omitempty"` // "1.0" or "2.0", defaults to "2.0" if not specified
}

func (ac *AthenaConfiguration) Validate() error {

	// Validate Authorizer
	if ac.Authorizer == nil {
		return fmt.Errorf("AthenaConfiguration: missing Authorizer")
	}

	err := ac.Authorizer.Validate()
	if err != nil {
		return fmt.Errorf("AthenaConfiguration: %s", err)
	}

	// Validate base properties
	if ac.Bucket == "" {
		return fmt.Errorf("AthenaConfiguration: missing bucket")
	}

	if ac.Region == "" {
		return fmt.Errorf("AthenaConfiguration: missing region")
	}

	if ac.Database == "" {
		return fmt.Errorf("AthenaConfiguration: missing database")
	}

	if ac.Table == "" {
		return fmt.Errorf("AthenaConfiguration: missing table")
	}

	if ac.Account == "" {
		return fmt.Errorf("AthenaConfiguration: missing account")
	}

	// Validate CURVersion if specified
	if ac.CURVersion != "" && ac.CURVersion != "1.0" && ac.CURVersion != "2.0" {
		return fmt.Errorf("AthenaConfiguration: invalid CURVersion '%s', must be '1.0' or '2.0'", ac.CURVersion)
	}

	return nil
}

func (ac *AthenaConfiguration) Equals(config cloud.Config) bool {
	if config == nil {
		return false
	}
	thatConfig, ok := config.(*AthenaConfiguration)
	if !ok {
		return false
	}

	if ac.Authorizer != nil {
		if !ac.Authorizer.Equals(thatConfig.Authorizer) {
			return false
		}
	} else {
		if thatConfig.Authorizer != nil {
			return false
		}
	}

	if ac.Bucket != thatConfig.Bucket {
		return false
	}

	if ac.Region != thatConfig.Region {
		return false
	}

	if ac.Database != thatConfig.Database {
		return false
	}

	if ac.Catalog != thatConfig.Catalog {
		return false
	}

	if ac.Table != thatConfig.Table {
		return false
	}

	if ac.Workgroup != thatConfig.Workgroup {
		return false
	}

	if ac.Account != thatConfig.Account {
		return false
	}

	if ac.CURVersion != thatConfig.CURVersion {
		return false
	}

	return true
}

func (ac *AthenaConfiguration) Sanitize() cloud.Config {
	return &AthenaConfiguration{
		Bucket:     ac.Bucket,
		Region:     ac.Region,
		Database:   ac.Database,
		Catalog:    ac.Catalog,
		Table:      ac.Table,
		Workgroup:  ac.Workgroup,
		Account:    ac.Account,
		Authorizer: ac.Authorizer.Sanitize().(Authorizer),
		CURVersion: ac.CURVersion,
	}
}

func (ac *AthenaConfiguration) Key() string {
	return fmt.Sprintf("%s/%s", ac.Account, ac.Bucket)
}

func (ac *AthenaConfiguration) Provider() string {
	return opencost.AWSProvider
}

func (ac *AthenaConfiguration) UnmarshalJSON(b []byte) error {
	var f interface{}
	err := json.Unmarshal(b, &f)
	if err != nil {
		return err
	}

	fmap := f.(map[string]interface{})

	bucket, err := cloud.GetInterfaceValue[string](fmap, "bucket")
	if err != nil {
		return fmt.Errorf("AthenaConfiguration: UnmarshalJSON: %w", err)
	}
	ac.Bucket = bucket

	region, err := cloud.GetInterfaceValue[string](fmap, "region")
	if err != nil {
		return fmt.Errorf("AthenaConfiguration: UnmarshalJSON: %w", err)
	}
	ac.Region = region

	database, err := cloud.GetInterfaceValue[string](fmap, "database")
	if err != nil {
		return fmt.Errorf("AthenaConfiguration: UnmarshalJSON: %w", err)
	}
	ac.Database = database

	if _, ok := fmap["catalog"]; ok {
		catalog, err := cloud.GetInterfaceValue[string](fmap, "catalog")
		if err != nil {
			return fmt.Errorf("AthenaConfiguration: UnmarshalJSON: %w", err)
		}
		ac.Catalog = catalog
	}

	table, err := cloud.GetInterfaceValue[string](fmap, "table")
	if err != nil {
		return fmt.Errorf("AthenaConfiguration: UnmarshalJSON: %w", err)
	}
	ac.Table = table

	workgroup, err := cloud.GetInterfaceValue[string](fmap, "workgroup")
	if err != nil {
		return fmt.Errorf("AthenaConfiguration: UnmarshalJSON: %w", err)
	}
	ac.Workgroup = workgroup

	account, err := cloud.GetInterfaceValue[string](fmap, "account")
	if err != nil {
		return fmt.Errorf("AthenaConfiguration: UnmarshalJSON: %w", err)
	}
	ac.Account = account

	authAny, ok := fmap["authorizer"]
	if !ok {
		return fmt.Errorf("AthenaConfiguration: UnmarshalJSON: missing authorizer")
	}
	authorizer, err := cloud.AuthorizerFromInterface(authAny, SelectAuthorizerByType)
	if err != nil {
		return fmt.Errorf("AthenaConfiguration: UnmarshalJSON: %w", err)
	}
	ac.Authorizer = authorizer

	// Parse CURVersion if present (optional field)
	if _, ok := fmap["curVersion"]; ok {
		curVersion, err := cloud.GetInterfaceValue[string](fmap, "curVersion")
		if err != nil {
			return fmt.Errorf("AthenaConfiguration: UnmarshalJSON: %w", err)
		}
		ac.CURVersion = curVersion
	} else {
		// Default to 2.0 if not specified
		ac.CURVersion = "2.0"
	}

	return nil
}

// ConvertAwsAthenaInfoToConfig takes a legacy config and generates a Config based on the presence of properties to match
// legacy behavior
func ConvertAwsAthenaInfoToConfig(aai AwsAthenaInfo) cloud.KeyedConfig {
	if aai.IsEmpty() {
		return nil
	}

	var authorizer Authorizer
	if aai.ServiceKeyName == "" && aai.ServiceKeySecret == "" {
		authorizer = &ServiceAccount{}
	} else {
		authorizer = &AccessKey{
			ID:     aai.ServiceKeyName,
			Secret: aai.ServiceKeySecret,
		}
	}

	// Wrap Authorizer with AssumeRole if MasterPayerArn is set
	if aai.MasterPayerARN != "" {
		authorizer = &AssumeRole{
			Authorizer: authorizer,
			RoleARN:    aai.MasterPayerARN,
		}
	}

	var config cloud.KeyedConfig
	if aai.AthenaTable != "" || aai.AthenaDatabase != "" {
		// Use CURVersion from config if specified, otherwise default to 2.0
		curVersion := aai.CURVersion
		if curVersion == "" {
			curVersion = "2.0"
		}
		config = &AthenaConfiguration{
			Bucket:     aai.AthenaBucketName,
			Region:     aai.AthenaRegion,
			Catalog:    aai.AthenaCatalog,
			Database:   aai.AthenaDatabase,
			Table:      aai.AthenaTable,
			Workgroup:  aai.AthenaWorkgroup,
			Account:    aai.AccountID,
			Authorizer: authorizer,
			CURVersion: curVersion,
		}
	} else {
		config = &S3Configuration{
			Bucket:     aai.AthenaBucketName,
			Region:     aai.AthenaRegion,
			Account:    aai.AccountID,
			Authorizer: authorizer,
		}
	}

	return config
}
