package allocation

import (
	"fmt"
	"strings"

	"github.com/opencost/opencost/core/pkg/autocomplete"
	coreallocation "github.com/opencost/opencost/core/pkg/autocomplete/allocation"
	"github.com/opencost/opencost/core/pkg/opencost"
)

func QueryAllocationAutocompleteFromSetRange(asr *opencost.AllocationSetRange, req autocomplete.Request) (*autocomplete.Response, error) {
	field, err := autocomplete.NormalizeRequest(&req, coreallocation.ValidateField, autocomplete.NormalizeOptions{
		EnsureLabelConfig: true,
	})
	if err != nil {
		return nil, err
	}

	var matcher opencost.AllocationMatcher
	if autocomplete.HasFilter(req.Filter) {
		compiler := opencost.NewAllocationMatchCompiler(req.LabelConfig)
		matcher, err = compiler.Compile(req.Filter)
		if err != nil {
			return nil, fmt.Errorf("%w: failed to compile filter: %w", autocomplete.ErrBadRequest, err)
		}
	}

	search := strings.ToLower(req.Search)
	results := map[string]struct{}{}
	for _, as := range asr.Allocations {
		if as == nil {
			continue
		}
		for _, alloc := range as.Allocations {
			if alloc == nil || alloc.Properties == nil {
				continue
			}
			if matcher != nil && !matcher.Matches(alloc) {
				continue
			}

			values := allocationAutocompleteValues(alloc.Properties, field)
			for _, value := range values {
				if value == "" {
					continue
				}
				if search != "" && !strings.Contains(strings.ToLower(value), search) {
					continue
				}
				results[value] = struct{}{}
			}
		}
	}

	return &autocomplete.Response{Data: autocomplete.UniqueSortedLimited(results, req.Limit)}, nil
}

func allocationAutocompleteValues(props *opencost.AllocationProperties, field string) []string {
	switch {
	case field == "account":
		return nil
	case field == "cluster":
		return []string{props.Cluster}
	case field == "namespace":
		return []string{props.Namespace}
	case field == "node":
		return []string{props.Node}
	case field == "controllerkind":
		return []string{props.ControllerKind}
	case field == "controllername":
		return []string{props.Controller}
	case field == "pod":
		return []string{props.Pod}
	case field == "container":
		return []string{props.Container}
	case field == "label":
		return mapKeys(props.Labels)
	case strings.HasPrefix(field, "label:"):
		label := strings.TrimPrefix(field, "label:")
		if v, ok := autocomplete.MapValueFold(props.Labels, label); ok {
			return []string{v}
		}
	case field == "namespacelabel":
		return mapKeys(props.NamespaceLabels)
	case strings.HasPrefix(field, "namespacelabel:"):
		label := strings.TrimPrefix(field, "namespacelabel:")
		if v, ok := autocomplete.MapValueFold(props.NamespaceLabels, label); ok {
			return []string{v}
		}
	}
	return nil
}

func mapKeys(values map[string]string) []string {
	result := make([]string, 0, len(values))
	for k := range values {
		result = append(result, k)
	}
	return result
}
