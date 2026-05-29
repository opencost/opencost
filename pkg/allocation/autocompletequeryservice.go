package allocation

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/opencost/opencost/core/pkg/filter"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/autocomplete"
)

// ErrAutocompleteBadRequest indicates a client error in an autocomplete request.
var ErrAutocompleteBadRequest = errors.New("autocomplete bad request")

// IsAutocompleteBadRequest reports whether err is a client validation error.
func IsAutocompleteBadRequest(err error) bool {
	return errors.Is(err, ErrAutocompleteBadRequest)
}

const DefaultAutocompleteResultLimit = 100
const MaxAutocompleteResultLimit = 1000

type AllocationAutocompleteRequest struct {
	Search      string
	Field       string
	Limit       int
	Window      opencost.Window
	Filter      filter.Filter
	LabelConfig *opencost.LabelConfig
}

type AllocationAutocompleteResponse struct {
	Data []string `json:"data"`
}

type AutocompleteQueryService interface {
	QueryAllocationAutocomplete(AllocationAutocompleteRequest, context.Context) (*AllocationAutocompleteResponse, error)
}

func QueryAllocationAutocompleteFromSetRange(asr *opencost.AllocationSetRange, req AllocationAutocompleteRequest) (*AllocationAutocompleteResponse, error) {
	field, err := NormalizeAllocationAutocompleteRequest(&req)
	if err != nil {
		return nil, err
	}

	var matcher opencost.AllocationMatcher
	if req.Filter != nil {
		compiler := opencost.NewAllocationMatchCompiler(req.LabelConfig)
		matcher, err = compiler.Compile(req.Filter)
		if err != nil {
			return nil, fmt.Errorf("%w: failed to compile filter: %w", ErrAutocompleteBadRequest, err)
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

	return &AllocationAutocompleteResponse{Data: autocomplete.UniqueSortedLimited(results, req.Limit)}, nil
}

// ValidateAutocompleteField normalizes and validates an allocation autocomplete field name.
func ValidateAutocompleteField(field string) (string, error) {
	if field == "" {
		return "", fmt.Errorf("field is required")
	}

	f := strings.ToLower(field)
	switch f {
	case "account", "cluster", "namespace", "node", "controllerkind", "controllername", "pod", "container", "label", "namespacelabel":
		return f, nil
	}

	if strings.HasPrefix(f, "label:") {
		_, labelKey, _ := strings.Cut(field, ":")
		return autocomplete.FormatLabelValueField(autocomplete.LabelPrefix, labelKey), nil
	}
	if strings.HasPrefix(f, "namespacelabel:") {
		_, labelKey, _ := strings.Cut(field, ":")
		return autocomplete.FormatLabelValueField(autocomplete.NamespaceLabelPrefix, labelKey), nil
	}

	return "", fmt.Errorf("unrecognized field: %s", field)
}

func allocationAutocompleteValues(props *opencost.AllocationProperties, field string) []string {
	switch {
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
