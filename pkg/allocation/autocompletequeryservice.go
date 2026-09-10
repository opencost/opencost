package allocation

import (
	"fmt"
	"strings"

	"github.com/opencost/opencost/core/pkg/autocomplete"
	coreallocation "github.com/opencost/opencost/core/pkg/autocomplete/allocation"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/promutil"
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

			values := allocationAutocompleteValues(alloc.Properties, field, req.LabelConfig)
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

func allocationAutocompleteValues(props *opencost.AllocationProperties, field string, labelConfig *opencost.LabelConfig) []string {
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
	// Alias fields resolve to user-configured label keys via LabelConfig.
	// Each configured key string may be comma-separated; all keys are checked.
	case field == "department", field == "environment", field == "owner", field == "product", field == "team":
		return aliasLabelValues(props.Labels, aliasLabelKey(field, labelConfig))
	}
	return nil
}

// aliasLabelKey returns the configured label key string for the given alias field.
func aliasLabelKey(alias string, labelConfig *opencost.LabelConfig) string {
	if labelConfig == nil {
		labelConfig = opencost.NewLabelConfig()
	}
	switch alias {
	case "department":
		return labelConfig.DepartmentLabel
	case "environment":
		return labelConfig.EnvironmentLabel
	case "owner":
		return labelConfig.OwnerLabel
	case "product":
		return labelConfig.ProductLabel
	case "team":
		return labelConfig.TeamLabel
	}
	return ""
}

// aliasLabelValues looks up each comma-separated key from configuredKey in labels
// and returns all found values.
func aliasLabelValues(labels map[string]string, configuredKey string) []string {
	if configuredKey == "" || len(labels) == 0 {
		return nil
	}
	var results []string
	for _, key := range strings.Split(configuredKey, ",") {
		key = promutil.SanitizeLabelName(strings.TrimSpace(key))
		if v, ok := labels[key]; ok && v != "" {
			results = append(results, v)
		}
	}
	return results
}

func mapKeys(values map[string]string) []string {
	result := make([]string, 0, len(values))
	for k := range values {
		result = append(result, k)
	}
	return result
}
