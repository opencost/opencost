package kubecost

import (
	"github.com/kubecost/cost-model/pkg/log"
	"strings"
)

const (
	CloudUsageNilProp string = ""
	CloudUsageProviderProp string = "provider"
	CloudUsageAccountProp string = "account"
	CloudUsageProjectProp string = "project"
	CloudUsageServiceProp string = "service"
	CloudUsageRegionProp string = "region"
	CloudUsageProviderIDProp string = "providerID"
)

// CloudUsageProperties contains
type CloudUsageProperties struct {
	Provider   string `json:"provider,omitempty"`
	Account    string `json:"account,omitempty"`
	Project    string `json:"project,omitempty"`
	Service    string `json:"service,omitempty"`
	Region     string `json:"region,omitempty"`
	ProviderID string `json:"providerID,omitempty"`
}

// Clone returns a deep copy of the calling CloudUsageProperties
func (cup *CloudUsageProperties) Clone() *CloudUsageProperties {
	if cup == nil {
		return nil
	}

	clone := &CloudUsageProperties{}
	clone.Provider = cup.Provider
	clone.Account = cup.Account
	clone.Project = cup.Project
	clone.Service = cup.Service
	clone.Region = cup.Region
	clone.ProviderID = cup.ProviderID

	return clone
}

// Equal determine if all fields of calling and provided CloudUsageProperties are equal
func (cup *CloudUsageProperties) Equal(that *CloudUsageProperties) bool {
	if cup == nil || that == nil {
		return false
	}

	if cup.Provider != that.Provider {
		return false
	}

	if cup.Account != that.Account {
		return false
	}
	if cup.Project != that.Project {
		return false
	}
	if cup.Service != that.Service {
		return false
	}
	if cup.Region != that.Region {
		return false
	}
	if cup.Region != that.Region {
		return false
	}

	return true
}

// GenerateKey generates a string that represents the key by which the
// CloudUsageProperties should be aggregated, given the properties defined by
// the aggregateBy parameter.
func (cup *CloudUsageProperties) GenerateKey(aggregateBy []string) string {
	if cup == nil {
		return ""
	}


	// Names will ultimately be joined into a single name, which uniquely
	// identifies cloudUsages.
	var names []string

	for _, agg := range aggregateBy {
		switch true {
		case agg == CloudUsageProviderProp:
			names = append(names, cup.Provider)
		case agg == CloudUsageAccountProp:
			names = append(names, cup.Account)
		case agg == CloudUsageProjectProp:
			names = append(names, cup.Project)
		case agg == CloudUsageServiceProp:
			names = append(names, cup.Service)
		case agg == CloudUsageRegionProp:
			names = append(names, cup.Region)
		case agg == CloudUsageProviderIDProp:
			names = append(names, cup.ProviderID)
		default:
			// This case should never be reached, as input up until this point
			// should be checked and rejected if invalid. But if we do get a
			// value we don't recognize, log a warning.
			log.Warningf("generateKey: illegal aggregation parameter: %s", agg)
		}
	}
	return strings.Join(names, "/")
}
