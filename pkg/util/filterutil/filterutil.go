package filterutil

import (
	"strings"

	"github.com/opencost/opencost/pkg/costmodel/clusters"
	"github.com/opencost/opencost/pkg/kubecost"
	"github.com/opencost/opencost/pkg/log"
	"github.com/opencost/opencost/pkg/prom"
	"github.com/opencost/opencost/pkg/util/typeutil"

	filter "github.com/opencost/opencost/pkg/filter21"
	afilter "github.com/opencost/opencost/pkg/filter21/allocation"
	"github.com/opencost/opencost/pkg/filter21/ast"
	// cloudfilter "github.com/opencost/opencost/pkg/filter/cloud"
)

// ============================================================================
// This file contains:
// Parsing (HTTP query params -> v2.1 filter) for V1 of query param filters
//
// e.g. "filterNamespaces=ku&filterControllers=deployment:kc"
// ============================================================================

// This is somewhat of a fancy solution, but allows us to "register" DefaultFieldByName funcs
// funcs by Field type.
var defaultFieldByType = map[string]any{
	// typeutil.TypeOf[cloudfilter.CloudAggregationField](): cloudfilter.DefaultFieldByName,
	typeutil.TypeOf[afilter.AllocationField](): afilter.DefaultFieldByName,
}

// DefaultFieldByName looks up a specific T field instance by name and returns the default
// ast.Field value for that type.
func DefaultFieldByName[T ~string](field T) *ast.Field {
	lookup, ok := defaultFieldByType[typeutil.TypeOf[T]()]
	if !ok {
		log.Errorf("Failed to get default field lookup for: %s", typeutil.TypeOf[T]())
		return nil
	}

	defaultLookup, ok := lookup.(func(T) *ast.Field)
	if !ok {
		log.Errorf("Failed to cast default field lookup for: %s", typeutil.TypeOf[T]())
		return nil
	}

	return defaultLookup(field)
}

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

// AllocationPropToV1FilterParamKey maps allocation string property
// representations to v1 filter param keys for legacy filter config support
// (e.g. reports). Example mapping: "cluster" -> "filterClusters"
var AllocationPropToV1FilterParamKey = map[string]string{
	kubecost.AllocationClusterProp:        ParamFilterClusters,
	kubecost.AllocationNodeProp:           ParamFilterNodes,
	kubecost.AllocationNamespaceProp:      ParamFilterNamespaces,
	kubecost.AllocationControllerProp:     ParamFilterControllers,
	kubecost.AllocationControllerKindProp: ParamFilterControllerKinds,
	kubecost.AllocationPodProp:            ParamFilterPods,
	kubecost.AllocationLabelProp:          ParamFilterLabels,
	kubecost.AllocationServiceProp:        ParamFilterServices,
	kubecost.AllocationDepartmentProp:     ParamFilterDepartments,
	kubecost.AllocationEnvironmentProp:    ParamFilterEnvironments,
	kubecost.AllocationOwnerProp:          ParamFilterOwners,
	kubecost.AllocationProductProp:        ParamFilterProducts,
	kubecost.AllocationTeamProp:           ParamFilterTeams,
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
	params AllocationFilterV1,
	labelConfig *kubecost.LabelConfig,
	clusterMap clusters.ClusterMap,
) filter.Filter {

	var filterOps []ast.FilterNode

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

	if len(params.Clusters) > 0 {
		var ops []ast.FilterNode

		// filter my cluster identifier
		ops = push(ops, filterV1SingleValueFromList(params.Clusters, afilter.FieldClusterID))

		for _, rawFilterValue := range params.Clusters {
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
				ops = append(ops, &ast.EqualOp{
					Left: ast.Identifier{
						Field: afilter.DefaultFieldByName(afilter.FieldClusterID),
						Key:   "",
					},
					Right: clusterID,
				})
			}
		}

		//
		clustersOp := opsToOr(ops)
		filterOps = push(filterOps, clustersOp)
	}

	if len(params.Nodes) > 0 {
		filterOps = push(filterOps, filterV1SingleValueFromList(params.Nodes, afilter.FieldNode))
	}

	if len(params.Namespaces) > 0 {
		filterOps = push(filterOps, filterV1SingleValueFromList(params.Namespaces, afilter.FieldNamespace))
	}

	if len(params.ControllerKinds) > 0 {
		filterOps = push(filterOps, filterV1SingleValueFromList(params.ControllerKinds, afilter.FieldControllerKind))
	}

	// filterControllers= accepts controllerkind:controllername filters, e.g.
	// "deployment:kubecost-cost-analyzer"
	//
	// Thus, we have to make a custom OR filter for this condition.
	if len(params.Controllers) > 0 {
		var ops []ast.FilterNode

		for _, rawFilterValue := range params.Controllers {
			split := strings.Split(rawFilterValue, ":")
			if len(split) == 1 {
				filterValue, wildcard := parseWildcardEnd(split[0])

				subFilter := toEqualOp(afilter.FieldControllerName, "", filterValue, wildcard)
				ops = append(ops, subFilter)
			} else if len(split) == 2 {
				kindFilterVal := split[0]
				nameFilterVal, wildcard := parseWildcardEnd(split[1])

				kindFilter := toEqualOp(afilter.FieldControllerKind, "", kindFilterVal, false)
				nameFilter := toEqualOp(afilter.FieldControllerName, "", nameFilterVal, wildcard)

				// The controller name AND the controller kind must match
				ops = append(ops, &ast.AndOp{
					Operands: []ast.FilterNode{
						kindFilter,
						nameFilter,
					},
				})
			} else {
				log.Warnf("illegal filter for controller: %s", rawFilterValue)
			}
		}
		controllersOp := opsToOr(ops)
		filterOps = push(filterOps, controllersOp)
	}

	if len(params.Pods) > 0 {
		filterOps = push(filterOps, filterV1SingleValueFromList(params.Pods, afilter.FieldPod))
	}

	if len(params.Containers) > 0 {
		filterOps = push(filterOps, filterV1SingleValueFromList(params.Containers, afilter.FieldContainer))
	}

	// Label-mapped queries require a label config to be present.
	if labelConfig != nil {
		if len(params.Departments) > 0 {
			filterOps = push(filterOps, filterV1LabelAliasMappedFromList(params.Departments, labelConfig.DepartmentLabel))
		}
		if len(params.Environments) > 0 {
			filterOps = push(filterOps, filterV1LabelAliasMappedFromList(params.Environments, labelConfig.EnvironmentLabel))
		}
		if len(params.Owners) > 0 {
			filterOps = push(filterOps, filterV1LabelAliasMappedFromList(params.Owners, labelConfig.OwnerLabel))
		}
		if len(params.Products) > 0 {
			filterOps = push(filterOps, filterV1LabelAliasMappedFromList(params.Products, labelConfig.ProductLabel))
		}
		if len(params.Teams) > 0 {
			filterOps = push(filterOps, filterV1LabelAliasMappedFromList(params.Teams, labelConfig.TeamLabel))
		}
	} else {
		log.Debugf("No label config is available. Not creating filters for label-mapped 'fields'.")
	}

	if len(params.Annotations) > 0 {
		filterOps = push(filterOps, filterV1DoubleValueFromList(params.Annotations, afilter.FieldAnnotation))
	}

	if len(params.Labels) > 0 {
		filterOps = push(filterOps, filterV1DoubleValueFromList(params.Labels, afilter.FieldLabel))
	}

	if len(params.Services) > 0 {
		var ops []ast.FilterNode

		// filterServices= is the only filter that uses the "contains" operator.
		for _, filterValue := range params.Services {
			// TODO: wildcard support
			filterValue, wildcard := parseWildcardEnd(filterValue)

			subFilter := toContainsOp(afilter.FieldServices, "", filterValue, wildcard)
			ops = append(ops, subFilter)
		}

		serviceOps := opsToOr(ops)
		filterOps = push(filterOps, serviceOps)
	}

	andFilter := opsToAnd(filterOps)
	if andFilter == nil {
		return &ast.VoidOp{} // no filter
	}

	return andFilter
}

// filterV1SingleValueFromList creates an OR of equality filters for a given
// filter field.
//
// The v1 query language (e.g. "filterNamespaces=XYZ,ABC") uses OR within
// a field (e.g. namespace = XYZ OR namespace = ABC)
func filterV1SingleValueFromList[T ~string](rawFilterValues []string, filterField T) ast.FilterNode {
	var ops []ast.FilterNode

	for _, filterValue := range rawFilterValues {
		filterValue = strings.TrimSpace(filterValue)
		filterValue, wildcard := parseWildcardEnd(filterValue)

		subFilter := toEqualOp(filterField, "", filterValue, wildcard)
		ops = append(ops, subFilter)
	}

	return opsToOr(ops)
}

// filterV1LabelAliasMappedFromList is like filterV1SingleValueFromList but is
// explicitly for labels and annotations because "label-mapped" filters (like filterTeams=)
// are actually label filters with a fixed label key.
func filterV1LabelAliasMappedFromList(rawFilterValues []string, labelName string) ast.FilterNode {
	var ops []ast.FilterNode
	labelName = prom.SanitizeLabelName(labelName)

	for _, filterValue := range rawFilterValues {
		filterValue = strings.TrimSpace(filterValue)
		filterValue, wildcard := parseWildcardEnd(filterValue)

		subFilter := toAllocationAliasOp(labelName, filterValue, wildcard)

		ops = append(ops, subFilter)
	}

	return opsToOr(ops)
}

// filterV1DoubleValueFromList creates an OR of key:value equality filters for
// colon-split filter values.
//
// The v1 query language (e.g. "filterLabels=app:foo,l2:bar") uses OR within
// a field (e.g. label[app] = foo OR label[l2] = bar)
func filterV1DoubleValueFromList(rawFilterValuesUnsplit []string, filterField afilter.AllocationField) ast.FilterNode {
	var ops []ast.FilterNode

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

			subFilter := toEqualOp(filterField, labelName, val, wildcard)
			ops = append(ops, subFilter)
		}
	}

	return opsToOr(ops)
}

// parseWildcardEnd checks if the given filter value is wildcarded, meaning
// it ends in "*". If it does, it removes the suffix and returns the cleaned
// string and true. Otherwise, it returns the same filter and false.
//
// parseWildcardEnd("kube*") = "kube", true
// parseWildcardEnd("kube") = "kube", false
func parseWildcardEnd(rawFilterValue string) (string, bool) {
	return strings.TrimSuffix(rawFilterValue, "*"), strings.HasSuffix(rawFilterValue, "*")
}

func push(a []ast.FilterNode, item ast.FilterNode) []ast.FilterNode {
	if item == nil {
		return a
	}

	return append(a, item)
}

func opsToOr(ops []ast.FilterNode) ast.FilterNode {
	if len(ops) == 0 {
		return nil
	}

	if len(ops) == 1 {
		return ops[0]
	}

	return &ast.OrOp{
		Operands: ops,
	}
}

func opsToAnd(ops []ast.FilterNode) ast.FilterNode {
	if len(ops) == 0 {
		return nil
	}

	if len(ops) == 1 {
		return ops[0]
	}

	return &ast.AndOp{
		Operands: ops,
	}
}

func toEqualOp[T ~string](field T, key string, value string, wildcard bool) ast.FilterNode {
	left := ast.Identifier{
		Field: DefaultFieldByName(field),
		Key:   key,
	}
	right := value

	if wildcard {
		return &ast.ContainsPrefixOp{
			Left:  left,
			Right: right,
		}
	}

	return &ast.EqualOp{
		Left:  left,
		Right: right,
	}
}

func toContainsOp[T ~string](field T, key string, value string, wildcard bool) ast.FilterNode {
	left := ast.Identifier{
		Field: DefaultFieldByName(field),
		Key:   key,
	}
	right := value

	if wildcard {
		return &ast.ContainsPrefixOp{
			Left:  left,
			Right: right,
		}
	}

	return &ast.ContainsOp{
		Left:  left,
		Right: right,
	}
}

func toAllocationAliasOp(labelName string, filterValue string, wildcard bool) *ast.OrOp {
	// labels.Contains(labelName)
	labelContainsKey := &ast.ContainsOp{
		Left: ast.Identifier{
			Field: afilter.DefaultFieldByName(afilter.FieldLabel),
			Key:   "",
		},
		Right: labelName,
	}

	// annotations.Contains(labelName)
	annotationContainsKey := &ast.ContainsOp{
		Left: ast.Identifier{
			Field: afilter.DefaultFieldByName(afilter.FieldAnnotation),
			Key:   "",
		},
		Right: labelName,
	}

	// labels[labelName] equals/startswith filterValue
	var labelSubFilter ast.FilterNode
	if wildcard {
		labelSubFilter = &ast.ContainsPrefixOp{
			Left: ast.Identifier{
				Field: afilter.DefaultFieldByName(afilter.FieldLabel),
				Key:   labelName,
			},
			Right: filterValue,
		}
	} else {
		labelSubFilter = &ast.EqualOp{
			Left: ast.Identifier{
				Field: afilter.DefaultFieldByName(afilter.FieldLabel),
				Key:   labelName,
			},
			Right: filterValue,
		}
	}

	// annotations[labelName] equals/startswith filterValue
	var annotationSubFilter ast.FilterNode
	if wildcard {
		annotationSubFilter = &ast.ContainsPrefixOp{
			Left: ast.Identifier{
				Field: afilter.DefaultFieldByName(afilter.FieldAnnotation),
				Key:   labelName,
			},
			Right: filterValue,
		}
	} else {
		annotationSubFilter = &ast.EqualOp{
			Left: ast.Identifier{
				Field: afilter.DefaultFieldByName(afilter.FieldAnnotation),
				Key:   labelName,
			},
			Right: filterValue,
		}
	}

	// Logically, this is equivalent to:
	// (labels.Contains(labelName) && labels[labelName] = filterValue) ||
	// (!labels.Contains(labelName) && annotations.Contains(labelName) && annotations[labelName] = filterValue)

	return &ast.OrOp{
		Operands: []ast.FilterNode{
			&ast.AndOp{
				Operands: []ast.FilterNode{
					labelContainsKey,
					labelSubFilter,
				},
			},
			&ast.AndOp{
				Operands: []ast.FilterNode{
					&ast.NotOp{
						Operand: ast.Clone(labelContainsKey),
					},
					annotationContainsKey,
					annotationSubFilter,
				},
			},
		},
	}
}
