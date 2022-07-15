package allocationfilterutil

import (
	"strings"

	"github.com/opencost/opencost/pkg/costmodel/clusters"
	"github.com/opencost/opencost/pkg/kubecost"
	"github.com/opencost/opencost/pkg/log"
	"github.com/opencost/opencost/pkg/prom"
	"github.com/opencost/opencost/pkg/util/httputil"
)

// ============================================================================
// This file contains:
// Parsing (HTTP query params -> AllocationFilter) for V1 of filters
//
// e.g. "filterNamespaces=ku&filterControllers=deployment:kc"
// ============================================================================

// parseWildcardEnd checks if the given filter value is wildcarded, meaning
// it ends in "*". If it does, it removes the suffix and returns the cleaned
// string and true. Otherwise, it returns the same filter and false.
//
// parseWildcardEnd("kube*") = "kube", true
// parseWildcardEnd("kube") = "kube", false
func parseWildcardEnd(rawFilterValue string) (string, bool) {
	return strings.TrimSuffix(rawFilterValue, "*"), strings.HasSuffix(rawFilterValue, "*")
}

// AllocationFilterFromParamsV1 takes a set of HTTP query parameters and
// converts them to an AllocationFilter, which is a structured in-Go
// representation of a set of filters.
//
// The HTTP query parameters are the "v1" filters attached to the Allocation
// API: "filterNamespaces=", "filterNodes=", etc.
//
// It takes an optional LabelConfig, which if provided enables "label-mapped"
// filters like "filterDepartments".
//
// It takes an optional ClusterMap, which if provided enables cluster name
// filtering. This turns all `filterClusters=foo` arguments into the equivalent
// of `clusterID = "foo" OR clusterName = "foo"`.
func AllocationFilterFromParamsV1(
	qp httputil.QueryParams,
	labelConfig *kubecost.LabelConfig,
	clusterMap clusters.ClusterMap,
) kubecost.AllocationFilter {

	filter := kubecost.AllocationFilterAnd{
		Filters: []kubecost.AllocationFilter{},
	}

	// ClusterMap does not provide a cluster name -> cluster ID mapping in the
	// interface, probably because there could be multiple IDs with the same
	// name. However, V1 filter logic demands that the parameters to
	// filterClusters= be checked against both cluster ID AND cluster name.
	//
	// To support expected filterClusters= behavior, we construct a mapping
	// of cluster name -> cluster IDs (could be multiple IDs for the same name)
	// so that we can create AllocationFilters that use only ClusterIDEquals.
	//
	//
	// AllocationFilter intentionally does not support cluster name filters
	// because those should be considered presentation-layer only.
	clusterNameToIDs := map[string][]string{}
	if clusterMap != nil {
		cMap := clusterMap.AsMap()
		for _, info := range cMap {
			if info == nil {
				continue
			}

			if _, ok := clusterNameToIDs[info.Name]; ok {
				clusterNameToIDs[info.Name] = append(clusterNameToIDs[info.Name], info.ID)
			} else {
				clusterNameToIDs[info.Name] = []string{info.ID}
			}
		}
	}

	// The proliferation of > 0 guards in the function is to avoid constructing
	// empty filter structs. While it is functionally equivalent to add empty
	// filter structs (they evaluate to true always) there could be overhead
	// when calling Matches() repeatedly for no purpose.

	if filterClusters := qp.GetList("filterClusters", ","); len(filterClusters) > 0 {
		clustersOr := kubecost.AllocationFilterOr{
			Filters: []kubecost.AllocationFilter{},
		}

		if idFilters := filterV1SingleValueFromList(filterClusters, kubecost.FilterClusterID); len(idFilters.Filters) > 0 {
			clustersOr.Filters = append(clustersOr.Filters, idFilters)
		}
		for _, rawFilterValue := range filterClusters {
			clusterNameFilter, wildcard := parseWildcardEnd(rawFilterValue)

			clusterIDsToFilter := []string{}
			for clusterName := range clusterNameToIDs {
				if wildcard && strings.HasPrefix(clusterName, clusterNameFilter) {
					clusterIDsToFilter = append(clusterIDsToFilter, clusterNameToIDs[clusterName]...)
				} else if !wildcard && clusterName == clusterNameFilter {
					clusterIDsToFilter = append(clusterIDsToFilter, clusterNameToIDs[clusterName]...)
				}
			}

			for _, clusterID := range clusterIDsToFilter {
				clustersOr.Filters = append(clustersOr.Filters,
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterClusterID,
						Op:    kubecost.FilterEquals,
						Value: clusterID,
					},
				)
			}
		}
		filter.Filters = append(filter.Filters, clustersOr)
	}

	if raw := qp.GetList("filterNodes", ","); len(raw) > 0 {
		filter.Filters = append(filter.Filters, filterV1SingleValueFromList(raw, kubecost.FilterNode))
	}

	if raw := qp.GetList("filterNamespaces", ","); len(raw) > 0 {
		filter.Filters = append(filter.Filters, filterV1SingleValueFromList(raw, kubecost.FilterNamespace))
	}

	if raw := qp.GetList("filterControllerKinds", ","); len(raw) > 0 {
		filter.Filters = append(filter.Filters, filterV1SingleValueFromList(raw, kubecost.FilterControllerKind))
	}

	// filterControllers= accepts controllerkind:controllername filters, e.g.
	// "deployment:kubecost-cost-analyzer"
	//
	// Thus, we have to make a custom OR filter for this condition.
	if filterControllers := qp.GetList("filterControllers", ","); len(filterControllers) > 0 {
		controllersOr := kubecost.AllocationFilterOr{
			Filters: []kubecost.AllocationFilter{},
		}
		for _, rawFilterValue := range filterControllers {
			split := strings.Split(rawFilterValue, ":")
			if len(split) == 1 {
				filterValue, wildcard := parseWildcardEnd(split[0])
				subFilter := kubecost.AllocationFilterCondition{
					Field: kubecost.FilterControllerName,
					Op:    kubecost.FilterEquals,
					Value: filterValue,
				}

				if wildcard {
					subFilter.Op = kubecost.FilterStartsWith
				}
				controllersOr.Filters = append(controllersOr.Filters, subFilter)
			} else if len(split) == 2 {
				kindFilterVal := split[0]
				nameFilterVal, wildcard := parseWildcardEnd(split[1])

				kindFilter := kubecost.AllocationFilterCondition{
					Field: kubecost.FilterControllerKind,
					Op:    kubecost.FilterEquals,
					Value: kindFilterVal,
				}
				nameFilter := kubecost.AllocationFilterCondition{
					Field: kubecost.FilterControllerName,
					Op:    kubecost.FilterEquals,
					Value: nameFilterVal,
				}

				if wildcard {
					nameFilter.Op = kubecost.FilterStartsWith
				}

				// The controller name AND the controller kind must match
				multiFilter := kubecost.AllocationFilterAnd{
					Filters: []kubecost.AllocationFilter{kindFilter, nameFilter},
				}
				controllersOr.Filters = append(controllersOr.Filters, multiFilter)
			} else {
				log.Warnf("illegal filter for controller: %s", rawFilterValue)
			}
		}
		if len(controllersOr.Filters) > 0 {
			filter.Filters = append(filter.Filters, controllersOr)
		}
	}

	if raw := qp.GetList("filterPods", ","); len(raw) > 0 {
		filter.Filters = append(filter.Filters, filterV1SingleValueFromList(raw, kubecost.FilterPod))
	}

	if raw := qp.GetList("filterContainers", ","); len(raw) > 0 {
		filter.Filters = append(filter.Filters, filterV1SingleValueFromList(raw, kubecost.FilterContainer))
	}

	// Label-mapped queries require a label config to be present.
	if labelConfig != nil {
		if raw := qp.GetList("filterDepartments", ","); len(raw) > 0 {
			filter.Filters = append(filter.Filters, filterV1LabelMappedFromList(raw, labelConfig.DepartmentLabel))
		}
		if raw := qp.GetList("filterEnvironments", ","); len(raw) > 0 {
			filter.Filters = append(filter.Filters, filterV1LabelMappedFromList(raw, labelConfig.EnvironmentLabel))
		}
		if raw := qp.GetList("filterOwners", ","); len(raw) > 0 {
			filter.Filters = append(filter.Filters, filterV1LabelMappedFromList(raw, labelConfig.OwnerLabel))
		}
		if raw := qp.GetList("filterProducts", ","); len(raw) > 0 {
			filter.Filters = append(filter.Filters, filterV1LabelMappedFromList(raw, labelConfig.ProductLabel))
		}
		if raw := qp.GetList("filterTeams", ","); len(raw) > 0 {
			filter.Filters = append(filter.Filters, filterV1LabelMappedFromList(raw, labelConfig.TeamLabel))
		}
	} else {
		log.Debugf("No label config is available. Not creating filters for label-mapped 'fields'.")
	}

	if raw := qp.GetList("filterAnnotations", ","); len(raw) > 0 {
		filter.Filters = append(filter.Filters, filterV1DoubleValueFromList(raw, kubecost.FilterAnnotation))
	}

	if raw := qp.GetList("filterLabels", ","); len(raw) > 0 {
		filter.Filters = append(filter.Filters, filterV1DoubleValueFromList(raw, kubecost.FilterLabel))
	}

	if filterServices := qp.GetList("filterServices", ","); len(filterServices) > 0 {
		// filterServices= is the only filter that uses the "contains" operator.
		servicesFilter := kubecost.AllocationFilterOr{
			Filters: []kubecost.AllocationFilter{},
		}
		for _, filterValue := range filterServices {
			// TODO: wildcard support
			filterValue, wildcard := parseWildcardEnd(filterValue)
			subFilter := kubecost.AllocationFilterCondition{
				Field: kubecost.FilterServices,
				Op:    kubecost.FilterContains,
				Value: filterValue,
			}
			if wildcard {
				subFilter.Op = kubecost.FilterContainsPrefix
			}
			servicesFilter.Filters = append(servicesFilter.Filters, subFilter)
		}
		filter.Filters = append(filter.Filters, servicesFilter)
	}

	return filter
}

// filterV1SingleValueFromList creates an OR of equality filters for a given
// filter field.
//
// The v1 query language (e.g. "filterNamespaces=XYZ,ABC") uses OR within
// a field (e.g. namespace = XYZ OR namespace = ABC)
func filterV1SingleValueFromList(rawFilterValues []string, filterField kubecost.FilterField) kubecost.AllocationFilterOr {
	filter := kubecost.AllocationFilterOr{
		Filters: []kubecost.AllocationFilter{},
	}

	for _, filterValue := range rawFilterValues {
		filterValue = strings.TrimSpace(filterValue)
		filterValue, wildcard := parseWildcardEnd(filterValue)

		subFilter := kubecost.AllocationFilterCondition{
			Field: filterField,
			// All v1 filters are equality comparisons
			Op:    kubecost.FilterEquals,
			Value: filterValue,
		}

		if wildcard {
			subFilter.Op = kubecost.FilterStartsWith
		}

		filter.Filters = append(filter.Filters, subFilter)
	}

	return filter
}

// filterV1LabelMappedFromList is like filterV1SingleValueFromList but is
// explicitly for a label because "label-mapped" filters (like filterTeams=)
// are actually label filters with a fixed label key.
func filterV1LabelMappedFromList(rawFilterValues []string, labelName string) kubecost.AllocationFilterOr {
	filter := kubecost.AllocationFilterOr{
		Filters: []kubecost.AllocationFilter{},
	}

	for _, filterValue := range rawFilterValues {
		filterValue = strings.TrimSpace(filterValue)
		filterValue, wildcard := parseWildcardEnd(filterValue)

		subFilter := kubecost.AllocationFilterCondition{
			Field: kubecost.FilterLabel,
			// All v1 filters are equality comparisons
			Op:    kubecost.FilterEquals,
			Key:   labelName,
			Value: filterValue,
		}

		if wildcard {
			subFilter.Op = kubecost.FilterStartsWith
		}

		filter.Filters = append(filter.Filters, subFilter)
	}

	return filter
}

// filterV1DoubleValueFromList creates an OR of key:value equality filters for
// colon-split filter values.
//
// The v1 query language (e.g. "filterLabels=app:foo,l2:bar") uses OR within
// a field (e.g. label[app] = foo OR label[l2] = bar)
func filterV1DoubleValueFromList(rawFilterValuesUnsplit []string, filterField kubecost.FilterField) kubecost.AllocationFilterOr {
	filter := kubecost.AllocationFilterOr{
		Filters: []kubecost.AllocationFilter{},
	}

	for _, unsplit := range rawFilterValuesUnsplit {
		if unsplit != "" {
			split := strings.Split(unsplit, ":")
			if len(split) != 2 {
				log.Warnf("illegal key/value filter (ignoring): %s", unsplit)
				continue
			}
			key := prom.SanitizeLabelName(strings.TrimSpace(split[0]))
			val := strings.TrimSpace(split[1])
			val, wildcard := parseWildcardEnd(val)

			subFilter := kubecost.AllocationFilterCondition{
				Field: filterField,
				// All v1 filters are equality comparisons
				Op:    kubecost.FilterEquals,
				Key:   key,
				Value: val,
			}

			if wildcard {
				subFilter.Op = kubecost.FilterStartsWith
			}

			filter.Filters = append(filter.Filters, subFilter)
		}
	}

	return filter
}
