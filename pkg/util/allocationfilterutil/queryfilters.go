package allocationfilterutil

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/opencost/opencost/pkg/costmodel/clusters"
	"github.com/opencost/opencost/pkg/kubecost"
	"github.com/opencost/opencost/pkg/log"
	"github.com/opencost/opencost/pkg/prom"
	"github.com/opencost/opencost/pkg/util/mapper"
)

const (
	ParamFilterClusters        = "filterClusters"
	ParamFilterNodes           = "filterNodes"
	ParamFilterNamespaces      = "filterNamespaces"
	ParamFilterControllerKinds = "filterControllerKinds"
	ParamFilterControllers     = "filterControllers"
	ParamFilterPods            = "filterPods"
	ParamFilterContainers      = "filterContainers"

	ParamFilterDepartments  = "filterDepartments"
	ParamFilterEnvironments = "filterEnvironments"
	ParamFilterOwners       = "filterOwners"
	ParamFilterProducts     = "filterProducts"
	ParamFilterTeams        = "filterTeams"

	ParamFilterAnnotations = "filterAnnotations"
	ParamFilterLabels      = "filterLabels"
	ParamFilterServices    = "filterServices"
)

var allocationFilterFieldMap = map[string]string{
	kubecost.AllocationClusterProp:        ParamFilterClusters,
	kubecost.FilterNode:                   ParamFilterNodes,
	kubecost.AllocationNamespaceProp:      ParamFilterNamespaces,
	kubecost.AllocationControllerKindProp: ParamFilterControllerKinds,
	kubecost.AllocationControllerProp:     ParamFilterControllers,
	kubecost.AllocationPodProp:            ParamFilterPods,
	kubecost.AllocationContainerProp:      ParamFilterContainers,
	kubecost.AllocationDepartmentProp:     ParamFilterDepartments,
	kubecost.AllocationEnvironmentProp:    ParamFilterEnvironments,
	kubecost.AllocationOwnerProp:          ParamFilterOwners,
	kubecost.AllocationProductProp:        ParamFilterProducts,
	kubecost.AllocationTeamProp:           ParamFilterTeams,
	kubecost.AllocationAnnotationProp:     ParamFilterAnnotations,
	kubecost.AllocationLabelProp:          ParamFilterLabels,
	kubecost.AllocationServiceProp:        ParamFilterServices,
}

func GetAllocationFilterForTheAllocationProperty(allocationProp string) (string, error) {
	if _, ok := allocationFilterFieldMap[allocationProp]; !ok {
		return "", fmt.Errorf("unknown allocation property %s", allocationProp)
	}
	return allocationFilterFieldMap[allocationProp], nil
}

// AllHTTPParamKeys returns all HTTP GET parameters used for v1 filters. It is
// intended to help validate HTTP queries in handlers to help avoid e.g.
// spelling errors.
func AllHTTPParamKeys() []string {
	return []string{
		ParamFilterClusters,
		ParamFilterNodes,
		ParamFilterNamespaces,
		ParamFilterControllerKinds,
		ParamFilterControllers,
		ParamFilterPods,
		ParamFilterContainers,

		ParamFilterDepartments,
		ParamFilterEnvironments,
		ParamFilterOwners,
		ParamFilterProducts,
		ParamFilterTeams,

		ParamFilterAnnotations,
		ParamFilterLabels,
		ParamFilterServices,
	}
}

type FilterV1 struct {
	Annotations     []string `json:"annotations"`
	Containers      []string `json:"containers"`
	Controllers     []string `json:"controllers"`
	ControllerKinds []string `json:"controllerKinds"`
	Clusters        []string `json:"clusters"`
	Departments     []string `json:"departments"`
	Environments    []string `json:"environments"`
	Labels          []string `json:"labels"`
	Namespaces      []string `json:"namespaces"`
	Nodes           []string `json:"nodes"`
	Owners          []string `json:"owners"`
	Pods            []string `json:"pods"`
	Products        []string `json:"products"`
	Services        []string `json:"services"`
	Teams           []string `json:"teams"`
}

func (f FilterV1) Equals(that FilterV1) bool {
	return reflect.DeepEqual(f.Annotations, that.Annotations) &&
		reflect.DeepEqual(f.Containers, that.Containers) &&
		reflect.DeepEqual(f.Controllers, that.Controllers) &&
		reflect.DeepEqual(f.ControllerKinds, that.ControllerKinds) &&
		reflect.DeepEqual(f.Clusters, that.Clusters) &&
		reflect.DeepEqual(f.Departments, that.Departments) &&
		reflect.DeepEqual(f.Environments, that.Environments) &&
		reflect.DeepEqual(f.Labels, that.Labels) &&
		reflect.DeepEqual(f.Namespaces, that.Namespaces) &&
		reflect.DeepEqual(f.Nodes, that.Nodes) &&
		reflect.DeepEqual(f.Owners, that.Owners) &&
		reflect.DeepEqual(f.Pods, that.Pods) &&
		reflect.DeepEqual(f.Products, that.Products) &&
		reflect.DeepEqual(f.Services, that.Services) &&
		reflect.DeepEqual(f.Teams, that.Teams)
}

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

// ParseAllocationFilterV1 takes a FilterV1 struct and
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
func ParseAllocationFilterV1(filters FilterV1, labelConfig *kubecost.LabelConfig, clusterMap clusters.ClusterMap) kubecost.AllocationFilter {
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

	if len(filters.Clusters) > 0 {
		clustersOr := kubecost.AllocationFilterOr{
			Filters: []kubecost.AllocationFilter{},
		}

		if idFilters := filterV1SingleValueFromList(filters.Clusters, kubecost.FilterClusterID); len(idFilters.Filters) > 0 {
			clustersOr.Filters = append(clustersOr.Filters, idFilters)
		}
		for _, rawFilterValue := range filters.Clusters {
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

	if len(filters.Nodes) > 0 {
		filter.Filters = append(filter.Filters, filterV1SingleValueFromList(filters.Nodes, kubecost.FilterNode))
	}

	if len(filters.Namespaces) > 0 {
		filter.Filters = append(filter.Filters, filterV1SingleValueFromList(filters.Namespaces, kubecost.FilterNamespace))
	}

	if len(filters.ControllerKinds) > 0 {
		filter.Filters = append(filter.Filters, filterV1SingleValueFromList(filters.ControllerKinds, kubecost.FilterControllerKind))
	}

	// filterControllers= accepts controllerkind:controllername filters, e.g.
	// "deployment:kubecost-cost-analyzer"
	//
	// Thus, we have to make a custom OR filter for this condition.
	if len(filters.Controllers) > 0 {
		controllersOr := kubecost.AllocationFilterOr{
			Filters: []kubecost.AllocationFilter{},
		}
		for _, rawFilterValue := range filters.Controllers {
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

	if len(filters.Pods) > 0 {
		filter.Filters = append(filter.Filters, filterV1SingleValueFromList(filters.Pods, kubecost.FilterPod))
	}

	if len(filters.Containers) > 0 {
		filter.Filters = append(filter.Filters, filterV1SingleValueFromList(filters.Containers, kubecost.FilterContainer))
	}

	// Label-mapped queries require a label config to be present.
	if labelConfig != nil {
		if len(filters.Departments) > 0 {
			filter.Filters = append(filter.Filters, filterV1LabelAliasMappedFromList(filters.Departments, labelConfig.DepartmentLabel))
		}
		if len(filters.Environments) > 0 {
			filter.Filters = append(filter.Filters, filterV1LabelAliasMappedFromList(filters.Environments, labelConfig.EnvironmentLabel))
		}
		if len(filters.Owners) > 0 {
			filter.Filters = append(filter.Filters, filterV1LabelAliasMappedFromList(filters.Owners, labelConfig.OwnerLabel))
		}
		if len(filters.Products) > 0 {
			filter.Filters = append(filter.Filters, filterV1LabelAliasMappedFromList(filters.Products, labelConfig.ProductLabel))
		}
		if len(filters.Teams) > 0 {
			filter.Filters = append(filter.Filters, filterV1LabelAliasMappedFromList(filters.Teams, labelConfig.TeamLabel))
		}
	} else {
		log.Debugf("No label config is available. Not creating filters for label-mapped 'fields'.")
	}

	if len(filters.Annotations) > 0 {
		filter.Filters = append(filter.Filters, filterV1DoubleValueFromList(filters.Annotations, kubecost.FilterAnnotation))
	}

	if len(filters.Labels) > 0 {
		filter.Filters = append(filter.Filters, filterV1DoubleValueFromList(filters.Labels, kubecost.FilterLabel))
	}

	if len(filters.Services) > 0 {
		// filterServices= is the only filter that uses the "contains" operator.
		servicesFilter := kubecost.AllocationFilterOr{
			Filters: []kubecost.AllocationFilter{},
		}
		for _, filterValue := range filters.Services {
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
	qp mapper.PrimitiveMapReader,
	labelConfig *kubecost.LabelConfig,
	clusterMap clusters.ClusterMap,
) kubecost.AllocationFilter {
	filter := ConvertFilterQueryParams(qp, labelConfig)
	return ParseAllocationFilterV1(filter, labelConfig, clusterMap)
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

func ConvertFilterQueryParams(qp mapper.PrimitiveMapReader, labelConfig *kubecost.LabelConfig) FilterV1 {
	filter := FilterV1{
		Annotations:     qp.GetList(ParamFilterAnnotations, ","),
		Containers:      qp.GetList(ParamFilterContainers, ","),
		Controllers:     qp.GetList(ParamFilterControllers, ","),
		ControllerKinds: qp.GetList(ParamFilterControllerKinds, ","),
		Clusters:        qp.GetList(ParamFilterClusters, ","),
		Labels:          qp.GetList(ParamFilterLabels, ","),
		Namespaces:      qp.GetList(ParamFilterNamespaces, ","),
		Nodes:           qp.GetList(ParamFilterNodes, ","),
		Pods:            qp.GetList(ParamFilterPods, ","),
		Services:        qp.GetList(ParamFilterServices, ","),
	}

	if labelConfig != nil {
		filter.Departments = qp.GetList(ParamFilterDepartments, ",")
		filter.Environments = qp.GetList(ParamFilterEnvironments, ",")
		filter.Owners = qp.GetList(ParamFilterOwners, ",")
		filter.Products = qp.GetList(ParamFilterProducts, ",")
		filter.Teams = qp.GetList(ParamFilterTeams, ",")
	} else {
		log.Debugf("No label config is available. Not creating filters for label-mapped 'fields'.")
	}

	return filter
}

// filterV1LabelAliasMappedFromList is like filterV1SingleValueFromList but is
// explicitly for labels and annotations because "label-mapped" filters (like filterTeams=)
// are actually label filters with a fixed label key.
func filterV1LabelAliasMappedFromList(rawFilterValues []string, labelName string) kubecost.AllocationFilterOr {
	filter := kubecost.AllocationFilterOr{
		Filters: []kubecost.AllocationFilter{},
	}
	labelName = prom.SanitizeLabelName(labelName)

	for _, filterValue := range rawFilterValues {
		filterValue = strings.TrimSpace(filterValue)
		filterValue, wildcard := parseWildcardEnd(filterValue)

		subFilter := kubecost.AllocationFilterCondition{
			Field: kubecost.FilterAlias,
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
			labelName := prom.SanitizeLabelName(strings.TrimSpace(split[0]))
			val := strings.TrimSpace(split[1])
			val, wildcard := parseWildcardEnd(val)

			subFilter := kubecost.AllocationFilterCondition{
				Field: filterField,
				// All v1 filters are equality comparisons
				Op:    kubecost.FilterEquals,
				Key:   labelName,
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
