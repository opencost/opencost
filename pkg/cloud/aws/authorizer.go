package aws

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/opencost/opencost/pkg/cloud/config"
	"github.com/opencost/opencost/pkg/util/json"
)

const AccessKeyAuthorizerType = "AWSAccessKey"
const ServiceAccountAuthorizerType = "AWSServiceAccount"
const AssumeRoleAuthorizerType = "AWSAssumeRole"

// Authorizer implementations provide aws.Config for AWS SDK calls
type Authorizer interface {
	config.Authorizer
	CreateAWSConfig(string) (aws.Config, error)
}

// SelectAuthorizerByType is an implementation of AuthorizerSelectorFn and acts as a register for Authorizer types
func SelectAuthorizerByType(typeStr string) (Authorizer, error) {
	switch typeStr {
	case AccessKeyAuthorizerType:
		return &AccessKey{}, nil
	case ServiceAccountAuthorizerType:
		return &ServiceAccount{}, nil
	case AssumeRoleAuthorizerType:
		return &AssumeRole{}, nil
	default:
		return nil, fmt.Errorf("AWS: provider authorizer type '%s' is not valid", typeStr)
	}
}

// AccessKey holds AWS credentials and fulfils the awsV2.CredentialsProvider interface
type AccessKey struct {
	ID     string `json:"id"`
	Secret string `json:"secret"`
}

// MarshalJSON custom json marshalling functions, sets properties as tagged in struct and sets the authorizer type property
func (ak *AccessKey) MarshalJSON() ([]byte, error) {
	fmap := make(map[string]any, 3)
	fmap[config.AuthorizerTypeProperty] = AccessKeyAuthorizerType
	fmap["id"] = ak.ID
	fmap["secret"] = ak.Secret
	return json.Marshal(fmap)
}

// Retrieve returns a set of awsV2 credentials using the AccessKey's key and secret.
// This fulfils the awsV2.CredentialsProvider interface contract.
func (ak *AccessKey) Retrieve(ctx context.Context) (aws.Credentials, error) {
	return aws.Credentials{
		AccessKeyID:     ak.ID,
		SecretAccessKey: ak.Secret,
	}, nil
}

func (ak *AccessKey) Validate() error {
	if ak.ID == "" {
		return fmt.Errorf("AccessKey: missing ID")
	}
	if ak.Secret == "" {
		return fmt.Errorf("AccessKey: missing Secret")
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

	if ak.ID != thatConfig.ID {
		return false
	}
	if ak.Secret != thatConfig.Secret {
		return false
	}
	return true
}

func (ak *AccessKey) Sanitize() config.Config {
	return &AccessKey{
		ID:     ak.ID,
		Secret: config.Redacted,
	}
}

// CreateAWSConfig creates an AWS SDK V2 Config for the credentials that it contains for the provided region
func (ak *AccessKey) CreateAWSConfig(region string) (cfg aws.Config, err error) {
	err = ak.Validate()
	if err != nil {
		return cfg, err
	}
	// The AWS SDK v2 requires an object fulfilling the CredentialsProvider interface, which cloud.AccessKey does
	cfg, err = awsconfig.LoadDefaultConfig(context.TODO(), awsconfig.WithCredentialsProvider(ak), awsconfig.WithRegion(region))
	if err != nil {
		return cfg, fmt.Errorf("failed to initialize AWS SDK config for region %s: %s", region, err)
	}
	return cfg, nil
}

// ServiceAccount uses pod annotations along with a service account to authenticate integrations
type ServiceAccount struct{}

// MarshalJSON custom json marshalling functions, sets properties as tagged in struct and sets the authorizer type property
func (sa *ServiceAccount) MarshalJSON() ([]byte, error) {
	fmap := make(map[string]any, 1)
	fmap[config.AuthorizerTypeProperty] = ServiceAccountAuthorizerType
	return json.Marshal(fmap)
}

// Check has nothing to check at this level, connection will fail if Pod Annotation and Service Account are not configured correctly
func (sa *ServiceAccount) Validate() error {
	return nil
}

func (sa *ServiceAccount) Equals(config config.Config) bool {
	if config == nil {
		return false
	}
	_, ok := config.(*ServiceAccount)
	if !ok {
		return false
	}

	return true
}

func (sa *ServiceAccount) Sanitize() config.Config {
	return &ServiceAccount{}
}

func (sa *ServiceAccount) CreateAWSConfig(region string) (aws.Config, error) {
	cfg, err := awsconfig.LoadDefaultConfig(context.TODO(), awsconfig.WithRegion(region))
	if err != nil {
		return cfg, fmt.Errorf("failed to initialize AWS SDK config for region from annotation %s: %s", region, err)
	}
	return cfg, nil
}

// AssumeRole is a wrapper for another Authorizer which adds an assumed role to the configuration
type AssumeRole struct {
	Authorizer Authorizer `json:"authorizer"`
	RoleARN    string     `json:"roleARN"`
}

// MarshalJSON custom json marshalling functions, sets properties as tagged in struct and sets the authorizer type property
func (ara *AssumeRole) MarshalJSON() ([]byte, error) {
	fmap := make(map[string]any, 3)
	fmap[config.AuthorizerTypeProperty] = AssumeRoleAuthorizerType
	fmap["roleARN"] = ara.RoleARN
	fmap["authorizer"] = ara.Authorizer
	return json.Marshal(fmap)
}

// UnmarshalJSON is required for AssumeRole because it needs to unmarshal an Authorizer interface
func (ara *AssumeRole) UnmarshalJSON(b []byte) error {
	var f interface{}
	err := json.Unmarshal(b, &f)
	if err != nil {
		return err
	}

	fmap := f.(map[string]interface{})

	roleARN, err := config.GetInterfaceValue[string](fmap, "roleARN")
	if err != nil {
		return fmt.Errorf("StorageConfiguration: UnmarshalJSON: %s", err.Error())
	}
	ara.RoleARN = roleARN

	authAny, ok := fmap["authorizer"]
	if !ok {
		return fmt.Errorf("AssumeRole: UnmarshalJSON: missing Authorizer")
	}
	authorizer, err := config.AuthorizerFromInterface(authAny, SelectAuthorizerByType)
	if err != nil {
		return fmt.Errorf("AssumeRole: UnmarshalJSON: %s", err.Error())
	}
	ara.Authorizer = authorizer

	return nil
}

func (ara *AssumeRole) CreateAWSConfig(region string) (aws.Config, error) {
	cfg, _ := ara.Authorizer.CreateAWSConfig(region)
	// Create the credentials from AssumeRoleProvider to assume the role
	// referenced by the RoleARN.
	stsSvc := sts.NewFromConfig(cfg)
	creds := stscreds.NewAssumeRoleProvider(stsSvc, ara.RoleARN)
	cfg.Credentials = aws.NewCredentialsCache(creds)
	return cfg, nil
}

func (ara *AssumeRole) Validate() error {
	if ara.Authorizer == nil {
		return fmt.Errorf("AssumeRole: misisng base Authorizer")
	}
	err := ara.Authorizer.Validate()
	if err != nil {
		return err
	}

	if ara.RoleARN == "" {
		return fmt.Errorf("AssumeRole: misisng RoleARN configuration")
	}

	return nil
}

func (ara *AssumeRole) Equals(config config.Config) bool {
	if config == nil {
		return false
	}
	thatConfig, ok := config.(*AssumeRole)
	if !ok {
		return false
	}
	if ara.Authorizer != nil {
		if !ara.Authorizer.Equals(thatConfig.Authorizer) {
			return false
		}
	} else {
		if thatConfig.Authorizer != nil {
			return false
		}
	}

	if ara.RoleARN != thatConfig.RoleARN {
		return false
	}

	return true
}

func (ara *AssumeRole) Sanitize() config.Config {
	return &AssumeRole{
		Authorizer: ara.Authorizer.Sanitize().(Authorizer),
		RoleARN:    ara.RoleARN,
	}
}
