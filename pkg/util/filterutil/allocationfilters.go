package filterutil

import (
	"strings"

	"github.com/kubecost/cost-model/pkg/kubecost"
	"github.com/kubecost/cost-model/pkg/log"
	"github.com/kubecost/cost-model/pkg/prom"
	"github.com/kubecost/cost-model/pkg/util/httputil"
)

// TODO: Make sure KCM callers provide a label config if possible/
// necessary
func FiltersFromParamsV1(qp httputil.QueryParams, labelConfig *kubecost.LabelConfig) kubecost.AllocationFilter {
	// TODO: wildcard handling

	filter := kubecost.AllocationFilterAnd{
		Filters: []kubecost.AllocationFilter{},
	}

	// TODO: remove comment
	// The following is adapted from KCM's original pkg/allocation/filters.go

	filter.Filters = append(filter.Filters,
		filterV1SingleValueFromList(qp.GetList("filterClusters", ","), kubecost.FilterClusterID),
	)
	// TODO: OR by cluster name
	// Cluster Map doesn't seem to have a name -> ID mapping,
	// only an ID (from the allocation) -> name mapping

	// generate a filter func for each node filter, and OR the results
	filter.Filters = append(filter.Filters,
		filterV1SingleValueFromList(qp.GetList("filterNodes", ","), kubecost.FilterNode),
	)

	// generate a filter func for each namespace filter, and OR the results
	filter.Filters = append(filter.Filters,
		filterV1SingleValueFromList(qp.GetList("filterNamespaces", ","), kubecost.FilterNamespace),
	)

	filter.Filters = append(filter.Filters,
		filterV1SingleValueFromList(qp.GetList("filterControllerKinds", ","), kubecost.FilterControllerKind),
	)

	// filterControllers= accepts controllerkind:controllername filters, e.g.
	// "deployment:kubecost-cost-analyzer"
	//
	// Thus, we have to make a custom OR filter for this condition.
	filterControllers := qp.GetList("filterControllers", ",")
	controllersOr := kubecost.AllocationFilterOr{
		Filters: []kubecost.AllocationFilter{},
	}
	for _, rawFilterValue := range filterControllers {
		split := strings.Split(rawFilterValue, ":")
		if len(split) == 1 {
			controllersOr.Filters = append(controllersOr.Filters,
				kubecost.AllocationFilterCondition{
					Field: kubecost.FilterControllerName,
					Op:    kubecost.FilterEquals,
					Value: split[0],
				})
		} else if len(split) == 2 {
			// The controller name AND the controller kind must match
			multiFilter := kubecost.AllocationFilterAnd{
				Filters: []kubecost.AllocationFilter{
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterControllerKind,
						Op:    kubecost.FilterEquals,
						Value: split[0],
					},
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterControllerName,
						Op:    kubecost.FilterEquals,
						Value: split[1],
					},
				},
			}
			controllersOr.Filters = append(controllersOr.Filters, multiFilter)
		} else {
			log.Warningf("illegal filter for controller: %s", rawFilterValue)
		}
	}
	filter.Filters = append(filter.Filters, controllersOr)

	filter.Filters = append(filter.Filters,
		filterV1SingleValueFromList(qp.GetList("filterPods", ","), kubecost.FilterPod),
	)

	filter.Filters = append(filter.Filters,
		filterV1SingleValueFromList(qp.GetList("filterContainers", ","), kubecost.FilterContainer),
	)

	// Label-mapped queries require a label config to be present.
	if labelConfig != nil {
		filter.Filters = append(filter.Filters,
			filterV1LabelMappedFromList(qp.GetList("filterDepartments", ","), labelConfig.DepartmentLabel),
		)
		filter.Filters = append(filter.Filters,
			filterV1LabelMappedFromList(qp.GetList("filterEnvironments", ","), labelConfig.EnvironmentLabel),
		)
		filter.Filters = append(filter.Filters,
			filterV1LabelMappedFromList(qp.GetList("filterOwners", ","), labelConfig.OwnerLabel),
		)
		filter.Filters = append(filter.Filters,
			filterV1LabelMappedFromList(qp.GetList("filterProducts", ","), labelConfig.ProductLabel),
		)
		filter.Filters = append(filter.Filters,
			filterV1LabelMappedFromList(qp.GetList("filterTeams", ","), labelConfig.TeamLabel),
		)
	} else {
		log.Debugf("No label config is available. Not creating filters for label-mapped 'fields'.")
	}

	filter.Filters = append(filter.Filters,
		filterV1DoubleValueFromList(qp.GetList("filterAnnotations", ","), kubecost.FilterAnnotation),
	)

	filter.Filters = append(filter.Filters,
		filterV1DoubleValueFromList(qp.GetList("filterLabels", ","), kubecost.FilterLabel),
	)

	// TODO: filter service condition
	// filterServices := qp.GetList("filterServices", ",")
	// if len(filterServices) > 0 {
	// 	subFilter := kubecost.AllocationFilterOr{
	// 		Filters: []kubecost.AllocationFilter{},
	// 	}

	// 	for _, filter := range filterServices {
	// 		ffs = append(ffs, GetServiceFilterFunc(filter))
	// 	}
	// 	filter.Filters = append(filter.Filters, subFilter)
	// }

	return filter
}

// TODO: comment
// We don't need the filter op because all filter V1 comparisons are equality
func filterV1SingleValueFromList(rawFilterValues []string, filterField kubecost.FilterField) kubecost.AllocationFilter {
	// The v1 query language (e.g. "filterNamespaces=XYZ,ABC") uses or within
	// a field (e.g. namespace = XYZ OR namespace = ABC)
	filter := kubecost.AllocationFilterOr{
		Filters: []kubecost.AllocationFilter{},
	}

	for _, filterValue := range rawFilterValues {
		filterValue = strings.TrimSpace(filterValue)

		filter.Filters = append(filter.Filters,
			kubecost.AllocationFilterCondition{
				Field: filterField,
				Op:    kubecost.FilterEquals,
				Value: filterValue,
			})
	}

	return filter
}

// TODO: comment
// We don't need the filter op because all filter V1 comparisons are equality
// We don't need the filter field because it is always a label
func filterV1LabelMappedFromList(rawFilterValues []string, labelName string) kubecost.AllocationFilter {
	// The v1 query language (e.g. "filterNamespaces=XYZ,ABC") uses or within
	// a field (e.g. namespace = XYZ OR namespace = ABC)
	filter := kubecost.AllocationFilterOr{
		Filters: []kubecost.AllocationFilter{},
	}

	for _, filterValue := range rawFilterValues {
		filterValue = strings.TrimSpace(filterValue)

		filter.Filters = append(filter.Filters,
			kubecost.AllocationFilterCondition{
				Field: kubecost.FilterLabel,
				Op:    kubecost.FilterEquals,
				Key:   labelName,
				Value: filterValue,
			})
	}

	return filter
}

// TODO: comment
// We don't need the filter op because all filter V1 comparisons are equality
func filterV1DoubleValueFromList(rawFilterValuesUnsplit []string, filterField kubecost.FilterField) kubecost.AllocationFilter {

	// The v1 query language (e.g. "filterLabels=app:foo,l2:bar") uses OR within
	// a field (e.g. label[app] = foo OR label[l2] = bar)
	filter := kubecost.AllocationFilterOr{
		Filters: []kubecost.AllocationFilter{},
	}

	for _, unsplit := range rawFilterValuesUnsplit {
		if unsplit != "" {
			split := strings.Split(unsplit, ":")
			if len(split) != 2 {
				log.Warningf("illegal key/value filter (ignoring): %s", unsplit)
				continue
			}
			key := prom.SanitizeLabelName(strings.TrimSpace(split[0]))
			val := strings.TrimSpace(split[1])

			filter.Filters = append(filter.Filters,
				kubecost.AllocationFilterCondition{
					Field: filterField,
					Op:    kubecost.FilterEquals,
					Key:   key,
					Value: val,
				},
			)
		}
	}

	return filter
}
