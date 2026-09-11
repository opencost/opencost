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

	// Alias fields resolve to configured label keys once per request rather
	// than once per allocation; nil for non-alias fields.
	aliasKeys := aliasLabelKeys(field, req.LabelConfig)

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

			values := allocationAutocompleteValues(alloc.Properties, field, aliasKeys)
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

func allocationAutocompleteValues(props *opencost.AllocationProperties, field string, aliasKeys []string) []string {
	prop := opencost.AllocationProperty(field)
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
	case prop.IsAliasedLabel():
		return aliasLabelValues(props, aliasKeys)
	}
	return nil
}

// aliasLabelKeys returns the sanitized label keys configured for an alias field
// (e.g. "team" -> LabelConfig.TeamLabel), or nil if field is not an alias. The
// configured value may be comma-separated. Keys are sanitized with
// LabelConfig.Sanitize, exactly as GenerateKey does, so autocomplete agrees with
// aggregation.
func aliasLabelKeys(field string, labelConfig *opencost.LabelConfig) []string {
	var configured string
	switch opencost.AllocationProperty(field) {
	case opencost.AllocationDepartmentProp:
		configured = labelConfig.DepartmentLabel
	case opencost.AllocationEnvironmentProp:
		configured = labelConfig.EnvironmentLabel
	case opencost.AllocationOwnerProp:
		configured = labelConfig.OwnerLabel
	case opencost.AllocationProductProp:
		configured = labelConfig.ProductLabel
	case opencost.AllocationTeamProp:
		configured = labelConfig.TeamLabel
	default:
		return nil
	}

	var keys []string
	for _, key := range strings.Split(configured, ",") {
		if key = labelConfig.Sanitize(key); key != "" {
			keys = append(keys, key)
		}
	}
	return keys
}

// aliasLabelValues returns the value of each alias key from the allocation's
// labels, falling back to its annotations when the label is absent. This
// mirrors GenerateKey and the alias filter pass, so every value returned here
// is one that aggregation and filtering will accept. Lookups are exact-case,
// as in GenerateKey; this intentionally differs from the case-folded
// label:<key> path.
func aliasLabelValues(props *opencost.AllocationProperties, keys []string) []string {
	var results []string
	for _, key := range keys {
		if v, ok := props.Labels[key]; ok {
			results = append(results, v)
			continue
		}
		if v, ok := props.Annotations[key]; ok {
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
