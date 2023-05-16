package gcp

import (
	"encoding/json"
	"fmt"

	"github.com/opencost/opencost/pkg/cloud/config"
	"google.golang.org/api/option"
)

const ServiceAccountKeyAuthorizerType = "GCPServiceAccountKey"
const WorkloadIdentityAuthorizerType = "GCPWorkloadIdentity"

// Authorizer provide a []option.ClientOption which is used in when creating clients in the GCP SDK
type Authorizer interface {
	config.Authorizer
	CreateGCPClientOptions() ([]option.ClientOption, error)
}

// SelectAuthorizerByType is an implementation of AuthorizerSelectorFn and acts as a register for Authorizer types
func SelectAuthorizerByType(typeStr string) (Authorizer, error) {
	switch typeStr {
	case ServiceAccountKeyAuthorizerType:
		return &ServiceAccountKey{}, nil
	case WorkloadIdentityAuthorizerType:
		return &WorkloadIdentity{}, nil
	default:
		return nil, fmt.Errorf("GCP: provider authorizer type '%s' is not valid", typeStr)
	}
}

type ServiceAccountKey struct {
	Key map[string]string `json:"key"`
}

// MarshalJSON custom json marshalling functions, sets properties as tagged in struct and sets the authorizer type property
func (gkc *ServiceAccountKey) MarshalJSON() ([]byte, error) {
	fmap := make(map[string]any, 2)
	fmap[config.AuthorizerTypeProperty] = ServiceAccountKeyAuthorizerType
	fmap["key"] = gkc.Key
	return json.Marshal(fmap)
}

func (gkc *ServiceAccountKey) Validate() error {
	if gkc.Key == nil || len(gkc.Key) == 0 {
		return fmt.Errorf("ServiceAccountKey: missing Key")
	}

	return nil
}

func (gkc *ServiceAccountKey) Equals(config config.Config) bool {
	if config == nil {
		return false
	}
	thatConfig, ok := config.(*ServiceAccountKey)
	if !ok {
		return false
	}

	if len(gkc.Key) != len(thatConfig.Key) {
		return false
	}

	for k, v := range gkc.Key {
		if thatConfig.Key[k] != v {
			return false
		}
	}

	return true
}

func (gkc *ServiceAccountKey) Sanitize() config.Config {
	redactedMap := make(map[string]string, len(gkc.Key))
	for key, _ := range gkc.Key {
		redactedMap[key] = config.Redacted
	}
	return &ServiceAccountKey{
		Key: redactedMap,
	}
}

func (gkc *ServiceAccountKey) CreateGCPClientOptions() ([]option.ClientOption, error) {
	err := gkc.Validate()
	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(gkc.Key)
	if err != nil {
		return nil, fmt.Errorf("Key: failed to marshal Key: %s", err.Error())
	}
	clientOption := option.WithCredentialsJSON(b)

	// The creation of the BigQuery Client is where FAILED_CONNECTION CloudConnectionStatus is recorded for GCP
	return []option.ClientOption{clientOption}, nil
}

// WorkloadIdentity passes an empty slice of client options which causes the GCP SDK to check for the workload identity in the environment
type WorkloadIdentity struct{}

// MarshalJSON custom json marshalling functions, sets properties as tagged in struct and sets the authorizer type property
func (wi *WorkloadIdentity) MarshalJSON() ([]byte, error) {
	fmap := make(map[string]any, 1)
	fmap[config.AuthorizerTypeProperty] = WorkloadIdentityAuthorizerType
	return json.Marshal(fmap)
}

func (wi *WorkloadIdentity) Validate() error {
	return nil
}

func (wi *WorkloadIdentity) Equals(config config.Config) bool {
	if config == nil {
		return false
	}
	_, ok := config.(*WorkloadIdentity)
	if !ok {
		return false
	}

	return true
}

func (wi *WorkloadIdentity) Sanitize() config.Config {
	return &WorkloadIdentity{}
}

func (wi *WorkloadIdentity) CreateGCPClientOptions() ([]option.ClientOption, error) {
	return []option.ClientOption{}, nil
}
