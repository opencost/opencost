package util

import (
	"strings"

	"github.com/opencost/opencost/pkg/filter"
	"github.com/opencost/opencost/pkg/kubecost"
	"github.com/opencost/opencost/pkg/log"
	"github.com/opencost/opencost/pkg/util/mapper"
)

func parseWildcardEnd(rawFilterValue string) (string, bool) {
	return strings.TrimSuffix(rawFilterValue, "*"), strings.HasSuffix(rawFilterValue, "*")
}

func CloudCostFilterFromParams(pmr mapper.PrimitiveMapReader) filter.Filter[*kubecost.CloudCost] {
	filter := filter.And[*kubecost.CloudCost]{
		Filters: []filter.Filter[*kubecost.CloudCost]{},
	}

	if raw := pmr.GetList("filterInvoiceEntityIDs", ","); len(raw) > 0 {
		filter.Filters = append(filter.Filters, filterV1SingleValueFromList(raw, kubecost.CloudCostInvoiceEntityIDProp))
	}

	if raw := pmr.GetList("filterAccountIDs", ","); len(raw) > 0 {
		filter.Filters = append(filter.Filters, filterV1SingleValueFromList(raw, kubecost.CloudCostAccountIDProp))
	}

	if raw := pmr.GetList("filterProviders", ","); len(raw) > 0 {
		filter.Filters = append(filter.Filters, filterV1SingleValueFromList(raw, kubecost.CloudCostProviderProp))
	}

	if raw := pmr.GetList("filterProviderIDs", ","); len(raw) > 0 {
		filter.Filters = append(filter.Filters, filterV1SingleValueFromList(raw, kubecost.CloudCostProviderIDProp))
	}

	if raw := pmr.GetList("filterServices", ","); len(raw) > 0 {
		filter.Filters = append(filter.Filters, filterV1SingleValueFromList(raw, kubecost.CloudCostServiceProp))
	}

	if raw := pmr.GetList("filterCategories", ","); len(raw) > 0 {
		filter.Filters = append(filter.Filters, filterV1SingleValueFromList(raw, kubecost.CloudCostCategoryProp))
	}

	if raw := pmr.GetList("filterLabels", ","); len(raw) > 0 {
		filter.Filters = append(filter.Filters, filterV1DoubleValueFromList(raw, kubecost.CloudCostLabelProp))
	}

	if len(filter.Filters) == 0 {
		return nil
	}

	return filter
}

func filterV1SingleValueFromList(rawFilterValues []string, field string) filter.Filter[*kubecost.CloudCost] {
	result := filter.Or[*kubecost.CloudCost]{
		Filters: []filter.Filter[*kubecost.CloudCost]{},
	}

	for _, filterValue := range rawFilterValues {
		filterValue = strings.TrimSpace(filterValue)
		filterValue, wildcard := parseWildcardEnd(filterValue)

		subFilter := filter.StringProperty[*kubecost.CloudCost]{
			Field: field,
			Op:    filter.StringEquals,
			Value: filterValue,
		}

		if wildcard {
			subFilter.Op = kubecost.FilterStartsWith
		}

		result.Filters = append(result.Filters, subFilter)
	}

	return result
}

// filterV1DoubleValueFromList creates an OR of key:value equality filters for
// colon-split filter values.
//
// The v1 query language (e.g. "filterLabels=app:foo,l2:bar") uses OR within
// a field (e.g. label[app] = foo OR label[l2] = bar)
func filterV1DoubleValueFromList(rawFilterValuesUnsplit []string, filterField string) filter.Filter[*kubecost.CloudCost] {
	result := filter.Or[*kubecost.CloudCost]{
		Filters: []filter.Filter[*kubecost.CloudCost]{},
	}

	for _, unsplit := range rawFilterValuesUnsplit {
		if unsplit != "" {
			split := strings.Split(unsplit, ":")
			if len(split) != 2 {
				log.Warnf("illegal key/value filter (ignoring): %s", unsplit)
				continue
			}
			labelName := strings.TrimSpace(split[0])
			val := strings.TrimSpace(split[1])
			val, wildcard := parseWildcardEnd(val)

			subFilter := filter.StringMapProperty[*kubecost.CloudCost]{
				Field: filterField,
				// All v1 filters are equality comparisons
				Op:    filter.StringMapEquals,
				Key:   labelName,
				Value: val,
			}

			if wildcard {
				subFilter.Op = filter.StringMapStartsWith
			}

			result.Filters = append(result.Filters, subFilter)
		}
	}

	return result
}
