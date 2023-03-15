package kubecost

import (
	"fmt"

	"github.com/opencost/opencost/pkg/filter/ast"
	cloudfilter "github.com/opencost/opencost/pkg/filter/cloud"
	"github.com/opencost/opencost/pkg/filter/matcher"
	"github.com/opencost/opencost/pkg/filter/transform"
)

// NewCloudAggregationMatchCompiler creates a new instance of a matcher.MatchCompiler[*CloudCostAggregate]
// which can be used to compile filter.Filter ASTs into matcher.Matcher[*CloudCostAggregate]
// implementations.
func NewCloudAggregationMatchCompiler() *matcher.MatchCompiler[*CloudCostAggregate] {
	return matcher.NewMatchCompiler(
		cloudCostFieldMap,
		cloudCostSliceFieldMap,
		cloudCostMapFieldMap,
		transform.PrometheusKeySanitizePass(),
		transform.UnallocatedReplacementPass())
}

// maps CloudCostAggregate types to the respective field definition
func cloudCostFieldMap(cca *CloudCostAggregate, identifier ast.Identifier) (string, error) {
	if cca == nil {
		return "", nil
	}

	switch cloudfilter.CloudAggregationField(identifier.Field.Name) {
	case cloudfilter.CloudAggregationFieldBilling:
		return cca.Properties.BillingID, nil
	case cloudfilter.CloudAggregationFieldWorkGroup:
		return cca.Properties.WorkGroupID, nil
	case cloudfilter.CloudAggregationFieldProvider:
		return cca.Properties.Provider, nil
	case cloudfilter.CloudAggregationFieldService:
		return cca.Properties.Service, nil
	case cloudfilter.CloudAggregationFieldLabel:
		return cca.Properties.LabelValue, nil
	default:
		return "", fmt.Errorf("invalid property name: %s", identifier.Field.Name)
	}
}

// Maps slice fields from an cloudcostaggregate to a []string value based on an identifier
func cloudCostSliceFieldMap(a *CloudCostAggregate, identifier ast.Identifier) ([]string, error) {
	return nil, fmt.Errorf("Failed to find []string identifier on CloudCostAggregate: %s", identifier.Field.Name)
}

// Maps slice fields from an cloudcostaggregate to a map[string]string value based on an identifier
func cloudCostMapFieldMap(a *CloudCostAggregate, identifier ast.Identifier) (map[string]string, error) {
	return nil, fmt.Errorf("Failed to find map[string]string identifier on CloudCostAggregate: %s", identifier.Field.Name)
}
