package kubecost

import (
	"fmt"
	"strings"

	"github.com/opencost/opencost/pkg/log"
)

const (
	CloudCostInvoiceEntityIDProp string = "invoiceEntityID"
	CloudCostAccountIDProp       string = "accountID"
	CloudCostProviderProp        string = "provider"
	CloudCostProviderIDProp      string = "providerID"
	CloudCostCategoryProp        string = "category"
	CloudCostServiceProp         string = "service"
	CloudCostLabelProp           string = "label"
)

const (
	// CloudCostClusterManagementCategory describes CloudCost representing Hosted Kubernetes Fees
	CloudCostClusterManagementCategory string = "Cluster Management"

	// CloudCostDiskCategory describes CloudCost representing Disk usage
	CloudCostDiskCategory string = "Disk"

	// CloudCostLoadBalancerCategory describes CloudCost representing Load Balancer usage
	CloudCostLoadBalancerCategory string = "Load Balancer"

	// CloudCostNetworkCategory describes CloudCost representing Network usage
	CloudCostNetworkCategory string = "Network"

	// CloudCostVirtualMachineCategory describes CloudCost representing VM usage
	CloudCostVirtualMachineCategory string = "Virtual Machine"

	// CloudCostOtherCategory describes CloudCost that do not belong to a defined category
	CloudCostOtherCategory string = "Other"
)

type CloudCostLabels map[string]string

func (ccl CloudCostLabels) Clone() CloudCostLabels {
	result := make(map[string]string, len(ccl))
	for k, v := range ccl {
		result[k] = v
	}
	return result
}

func (ccl CloudCostLabels) Equal(that CloudCostLabels) bool {
	if len(ccl) != len(that) {
		return false
	}

	// Maps are of equal length, so if all keys are in both maps, we don't
	// have to check the keys of the other map.
	for k, val := range ccl {
		if thatVal, ok := that[k]; !ok || val != thatVal {
			return false
		}
	}

	return true
}

// Intersection returns the set of labels that have the same key and value in the receiver and arg
func (ccl CloudCostLabels) Intersection(that CloudCostLabels) CloudCostLabels {
	intersection := make(map[string]string)
	if len(ccl) == 0 || len(that) == 0 {
		return intersection
	}

	// Pick the smaller of the two label sets
	smallerLabels := ccl
	largerLabels := that
	if len(ccl) > len(that) {
		smallerLabels = that
		largerLabels = ccl
	}

	// Loop through the smaller label set
	for k, sVal := range smallerLabels {
		if lVal, ok := largerLabels[k]; ok && sVal == lVal {
			intersection[k] = sVal
		}
	}
	return intersection
}

type CloudCostProperties struct {
	ProviderID      string          `json:"providerID,omitempty"`
	Provider        string          `json:"provider,omitempty"`
	AccountID       string          `json:"accountID,omitempty"`
	InvoiceEntityID string          `json:"invoiceEntityID,omitempty"`
	Service         string          `json:"service,omitempty"`
	Category        string          `json:"category,omitempty"`
	Labels          CloudCostLabels `json:"labels,omitempty"`
}

func (ccp *CloudCostProperties) Equal(that *CloudCostProperties) bool {
	return ccp.ProviderID == that.ProviderID &&
		ccp.Provider == that.Provider &&
		ccp.AccountID == that.AccountID &&
		ccp.InvoiceEntityID == that.InvoiceEntityID &&
		ccp.Service == that.Service &&
		ccp.Category == that.Category &&
		ccp.Labels.Equal(that.Labels)
}

func (ccp *CloudCostProperties) Clone() *CloudCostProperties {
	return &CloudCostProperties{
		ProviderID:      ccp.ProviderID,
		Provider:        ccp.Provider,
		AccountID:       ccp.AccountID,
		InvoiceEntityID: ccp.InvoiceEntityID,
		Service:         ccp.Service,
		Category:        ccp.Category,
		Labels:          ccp.Labels.Clone(),
	}
}

// Intersection ensure the values of two CloudCostAggregateProperties are maintain only if they are equal
func (ccp *CloudCostProperties) Intersection(that *CloudCostProperties) *CloudCostProperties {
	if ccp == nil || that == nil {
		return nil
	}

	if ccp.Equal(that) {
		return ccp
	}
	intersectionCCP := &CloudCostProperties{}
	if ccp.Equal(intersectionCCP) || that.Equal(intersectionCCP) {
		return intersectionCCP
	}

	if ccp.Provider == that.Provider {
		intersectionCCP.Provider = ccp.Provider
	}
	if ccp.ProviderID == that.ProviderID {
		intersectionCCP.ProviderID = ccp.ProviderID
	}
	if ccp.AccountID == that.AccountID {
		intersectionCCP.AccountID = ccp.AccountID
	}
	if ccp.InvoiceEntityID == that.InvoiceEntityID {
		intersectionCCP.InvoiceEntityID = ccp.InvoiceEntityID
	}
	if ccp.Service == that.Service {
		intersectionCCP.Service = ccp.Service
	}
	if ccp.Category == that.Category {
		intersectionCCP.Category = ccp.Category
	}
	intersectionCCP.Labels = ccp.Labels.Intersection(that.Labels)

	return intersectionCCP
}

func (ccp *CloudCostProperties) GenerateKey(props []string) string {

	if len(props) == 0 {
		return fmt.Sprintf("%s/%s/%s/%s/%s/%s", ccp.Provider, ccp.InvoiceEntityID, ccp.AccountID, ccp.Category, ccp.Service, ccp.ProviderID)
	}

	values := make([]string, len(props))
	for i, prop := range props {
		propVal := UnallocatedSuffix

		switch true {
		case prop == CloudCostProviderProp:
			if ccp.Provider != "" {
				propVal = ccp.Provider
			}
		case prop == CloudCostProviderIDProp:
			if ccp.ProviderID != "" {
				propVal = ccp.ProviderID
			}
		case prop == CloudCostCategoryProp:
			if ccp.Category != "" {
				propVal = ccp.Category
			}
		case prop == CloudCostInvoiceEntityIDProp:
			if ccp.InvoiceEntityID != "" {
				propVal = ccp.InvoiceEntityID
			}
		case prop == CloudCostAccountIDProp:
			if ccp.AccountID != "" {
				propVal = ccp.AccountID
			}
		case prop == CloudCostServiceProp:
			if ccp.Service != "" {
				propVal = ccp.Service
			}
		case strings.HasPrefix(prop, "label:"):
			labels := ccp.Labels
			if labels != nil {
				labelName := strings.TrimPrefix(prop, "label:")
				if labelValue, ok := labels[labelName]; ok && labelValue != "" {
					propVal = labelValue
				}
			}
		default:
			// This case should never be reached, as input up until this point
			// should be checked and rejected if invalid. But if we do get a
			// value we don't recognize, log a warning.
			log.Warnf("CloudCost: GenerateKey: illegal aggregation parameter: %s", prop)

		}

		values[i] = propVal
	}

	return strings.Join(values, "/")
}
