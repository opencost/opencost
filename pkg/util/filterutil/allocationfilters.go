package filterutil

import (
	"strings"

	"github.com/kubecost/cost-model/pkg/kubecost"
	"github.com/kubecost/cost-model/pkg/log"
	"github.com/kubecost/cost-model/pkg/prom"
	"github.com/kubecost/cost-model/pkg/util/httputil"
)

func FiltersFromParamsV1(qp httputil.QueryParams) kubecost.AllocationFilter {
	if qp.Get("filter", "") != "" {
		// TODO: short-circuit to a query language parser if the filter= param is
		// present.
	}

	// TODO: wildcard handling

	filter := kubecost.AllocationFilterAnd{
		Filters: []kubecost.AllocationFilter{},
	}

	// TODO: remove comment
	// The following is adapted from KCM's original pkg/allocation/filters.go

	// Load Label Config
	// Pull a LabelConfig from the app configuration, or default if
	// configuration is unavailable.
	// labelConfig := kubecost.NewLabelConfig()

	// TODO: label config from analyzer in OSS?
	// cfg, err := config.GetAnalyzerConfig()
	// if err != nil {
	// 	log.Warnf("AnalyzerConfig is nil")
	// } else {
	// 	labelConfig = cfg.LabelConfig()
	// }

	filterClusters := qp.GetList("filterClusters", ",")
	filter.Filters = append(filter.Filters,
		filterV1SingleValueFromList(filterClusters, kubecost.FilterClusterID),
	)
	// TODO: OR by cluster name
	// Cluster Map doesn't seem to have a name -> ID mapping,
	// only an ID (from the allocation) -> name mapping

	// generate a filter func for each node filter, and OR the results
	filterNodes := qp.GetList("filterNodes", ",")
	filter.Filters = append(filter.Filters,
		filterV1SingleValueFromList(filterNodes, kubecost.FilterNode),
	)

	// generate a filter func for each namespace filter, and OR the results
	filterNamespaces := qp.GetList("filterNamespaces", ",")
	filter.Filters = append(filter.Filters,
		filterV1SingleValueFromList(filterNamespaces, kubecost.FilterNamespace),
	)

	filterControllerKinds := qp.GetList("filterControllerKinds", ",")
	filter.Filters = append(filter.Filters,
		filterV1SingleValueFromList(filterControllerKinds, kubecost.FilterControllerKind),
	)

	filterControllers := qp.GetList("filterControllers", ",")
	// filterControllers= accepts controllerkind:controllername filters, e.g.
	// "deployment:kubecost-cost-analyzer"
	//
	// Thus, we have to make a custom OR filter for this condition.
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

	filterPods := qp.GetList("filterPods", ",")
	filter.Filters = append(filter.Filters,
		filterV1SingleValueFromList(filterPods, kubecost.FilterPod),
	)

	filterContainers := qp.GetList("filterContainers", ",")
	filter.Filters = append(filter.Filters,
		filterV1SingleValueFromList(filterContainers, kubecost.FilterContainer),
	)

	// TODO: label mapping special things
	// filterDepartments := qp.GetList("filterDepartments", ",")
	// if len(filterDepartments) > 0 {
	// 	subFilter := kubecost.AllocationFilterOr{
	// 		Filters: []kubecost.AllocationFilter{},
	// 	}

	// 	for _, filter := range filterDepartments {
	// 		ffs = append(ffs, GetDepartmentFilterFunc(labelConfig, filter))
	// 	}
	// 	filter.Filters = append(filter.Filters, subFilter)
	// }

	// filterEnvironments := qp.GetList("filterEnvironments", ",")
	// if len(filterEnvironments) > 0 {
	// 	subFilter := kubecost.AllocationFilterOr{
	// 		Filters: []kubecost.AllocationFilter{},
	// 	}

	// 	for _, filter := range filterEnvironments {
	// 		ffs = append(ffs, GetEnvironmentFilterFunc(labelConfig, filter))
	// 	}
	// 	filter.Filters = append(filter.Filters, subFilter)
	// }

	// filterOwners := qp.GetList("filterOwners", ",")
	// if len(filterOwners) > 0 {
	// 	subFilter := kubecost.AllocationFilterOr{
	// 		Filters: []kubecost.AllocationFilter{},
	// 	}

	// 	for _, filter := range filterOwners {
	// 		ffs = append(ffs, GetOwnerFilterFunc(labelConfig, filter))
	// 	}
	// 	filter.Filters = append(filter.Filters, subFilter)
	// }

	// filterProducts := qp.GetList("filterProducts", ",")
	// if len(filterProducts) > 0 {
	// 	subFilter := kubecost.AllocationFilterOr{
	// 		Filters: []kubecost.AllocationFilter{},
	// 	}

	// 	for _, filter := range filterProducts {
	// 		ffs = append(ffs, GetProductFilterFunc(labelConfig, filter))
	// 	}
	// 	filter.Filters = append(filter.Filters, subFilter)
	// }

	// filterTeams := qp.GetList("filterTeams", ",")
	// if len(filterTeams) > 0 {
	// 	subFilter := kubecost.AllocationFilterOr{
	// 		Filters: []kubecost.AllocationFilter{},
	// 	}

	// 	for _, filter := range filterTeams {
	// 		ffs = append(ffs, GetTeamFilterFunc(labelConfig, filter))
	// 	}
	// 	filter.Filters = append(filter.Filters, subFilter)
	// }

	filterAnnotations := qp.GetList("filterAnnotations", ",")
	filter.Filters = append(filter.Filters,
		filterV1DoubleValueFromList(filterAnnotations, kubecost.FilterAnnotation),
	)

	filterLabels := qp.GetList("filterLabels", ",")
	filter.Filters = append(filter.Filters,
		filterV1DoubleValueFromList(filterLabels, kubecost.FilterLabel),
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
