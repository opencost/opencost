package util

import (
	"strings"

	"github.com/opencost/opencost/pkg/filter"
	"github.com/opencost/opencost/pkg/kubecost"
	"github.com/opencost/opencost/pkg/util/mapper"
)

func parseWildcardEnd(rawFilterValue string) (string, bool) {
	return strings.TrimSuffix(rawFilterValue, "*"), strings.HasSuffix(rawFilterValue, "*")
}

func CloudCostAggregateFilterFromParams(pmr mapper.PrimitiveMapReader) filter.Filter[*kubecost.CloudCostAggregate] {
	filter := filter.And[*kubecost.CloudCostAggregate]{
		Filters: []filter.Filter[*kubecost.CloudCostAggregate]{},
	}

	if raw := pmr.GetList("filterBillingIDs", ","); len(raw) > 0 {
		filter.Filters = append(filter.Filters, filterV1SingleValueFromList(raw, kubecost.CloudCostBillingIDProp))
	}

	if raw := pmr.GetList("filterWorkGroupIDs", ","); len(raw) > 0 {
		filter.Filters = append(filter.Filters, filterV1SingleValueFromList(raw, kubecost.CloudCostWorkGroupIDProp))
	}

	if raw := pmr.GetList("filterProviders", ","); len(raw) > 0 {
		filter.Filters = append(filter.Filters, filterV1SingleValueFromList(raw, kubecost.CloudCostProviderProp))
	}

	if raw := pmr.GetList("filterServices", ","); len(raw) > 0 {
		filter.Filters = append(filter.Filters, filterV1SingleValueFromList(raw, kubecost.CloudCostServiceProp))
	}

	if raw := pmr.GetList("filterLabelValues", ","); len(raw) > 0 {
		filter.Filters = append(filter.Filters, filterV1SingleValueFromList(raw, kubecost.CloudCostLabelProp))
	}

	if len(filter.Filters) == 0 {
		return nil
	}

	return filter
}

func filterV1SingleValueFromList(rawFilterValues []string, field string) filter.Filter[*kubecost.CloudCostAggregate] {
	result := filter.Or[*kubecost.CloudCostAggregate]{
		Filters: []filter.Filter[*kubecost.CloudCostAggregate]{},
	}

	for _, filterValue := range rawFilterValues {
		filterValue = strings.TrimSpace(filterValue)
		filterValue, wildcard := parseWildcardEnd(filterValue)

		subFilter := filter.StringProperty[*kubecost.CloudCostAggregate]{
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
