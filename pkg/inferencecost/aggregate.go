package inferencecost

import (
	"fmt"
	"strings"

	"github.com/opencost/opencost/core/pkg/opencost"
)

// supportedAggregateProperties lists the InferenceCostProperties dimensions
// that the collector actually populates in Phase 1.
var supportedAggregateProperties = map[string]bool{
	"model_name":      true,
	"model_version":   true,
	"namespace":       true,
	"cluster":         true,
	"pod":             true,
	"controller":      true,
	"controller_kind": true,
	"container":       true,
}

// aggKey derives the aggregation map key for an InferenceCostResponse given
// the requested aggregation dimensions. Returns an error if any dimension is
// not in supportedAggregateProperties.
func aggKey(props InferenceCostAPIProperties, aggregateBy []string) (string, error) {
	if len(aggregateBy) == 0 {
		// Use "model:namespace" as the natural key. ":" cannot appear in
		// Kubernetes label values or namespace names, so this is unambiguous
		// even when the model name contains "/" (e.g. "org/model").
		ns := props.Namespace
		if ns == "" {
			ns = opencost.UnallocatedSuffix
		}
		return props.ModelName + ":" + ns, nil
	}

	parts := make([]string, 0, len(aggregateBy))
	for _, dim := range aggregateBy {
		if !supportedAggregateProperties[dim] {
			return "", fmt.Errorf("unsupported aggregation dimension %q: supported dimensions are model_name, model_version, namespace, cluster, pod, controller, controller_kind, container", dim)
		}
		var val string
		switch dim {
		case "model_name":
			val = props.ModelName
		case "model_version":
			val = props.ModelVersion
		case "namespace":
			val = props.Namespace
		case "cluster":
			val = props.Cluster
		case "pod":
			val = props.Pod
		case "controller":
			val = props.Controller
		case "controller_kind":
			val = props.ControllerKind
		case "container":
			val = props.Container
		}
		if val == "" {
			val = opencost.UnallocatedSuffix
		}
		parts = append(parts, val)
	}
	return strings.Join(parts, "/"), nil
}

// addResponse merges src into dst, summing additive fields and recomputing
// derived per-million-token rates from the summed numerators/denominators.
// Rates are NOT averaged — they are recomputed after summing to preserve
// accuracy (e.g. two models with different throughput cannot be averaged).
func addResponse(dst, src *InferenceCostResponse) {
	dst.TotalCost += src.TotalCost
	dst.PromptTokens += src.PromptTokens
	dst.GenerationTokens += src.GenerationTokens
	dst.TotalTokens += src.TotalTokens
	dst.InputCost += src.InputCost
	dst.OutputCost += src.OutputCost
	dst.cachedTokens += src.cachedTokens

	// Recompute blended rate from accumulated totals.
	if dst.TotalTokens > 0 {
		dst.CostPerMillionTokens = dst.TotalCost / dst.TotalTokens * 1_000_000
	} else {
		dst.CostPerMillionTokens = 0
	}

	if dst.PromptTokens > 0 {
		dst.InputCostPerMillionTokens = dst.InputCost / dst.PromptTokens * 1_000_000
		dst.CacheSavingsFraction = dst.cachedTokens / dst.PromptTokens
	} else {
		dst.InputCostPerMillionTokens = 0
		dst.CacheSavingsFraction = 0
	}

	if dst.GenerationTokens > 0 {
		dst.OutputCostPerMillionTokens = dst.OutputCost / dst.GenerationTokens * 1_000_000
	} else {
		dst.OutputCostPerMillionTokens = 0
	}

	// Preserve the allocation method from the first entry; clear it when
	// methods differ (the merged entry reflects a mixed derivation).
	if dst.AllocationMethod != src.AllocationMethod {
		dst.AllocationMethod = ""
	}
}

// aggregate groups the InferenceCosts in s by the given dimensions, summing
// all additive fields and recomputing derived rates. If aggregateBy is empty
// the set is returned unchanged.
//
// Properties that are not part of the aggregation key are cleared on the
// merged entry so the response accurately reflects what was grouped on.
// For example, aggregating by "namespace" clears modelName so the result
// doesn't show an arbitrary model name from whichever entry merged last.
func (s *InferenceCostSet) aggregate(aggregateBy []string) error {
	if len(aggregateBy) == 0 {
		return nil
	}

	aggDims := make(map[string]bool, len(aggregateBy))
	for _, d := range aggregateBy {
		aggDims[d] = true
	}

	aggMap := make(map[string]*InferenceCostResponse, len(s.InferenceCosts))
	for _, ic := range s.InferenceCosts {
		key, err := aggKey(ic.Properties, aggregateBy)
		if err != nil {
			return err
		}
		if existing, ok := aggMap[key]; ok {
			addResponse(existing, ic)
		} else {
			// Clone so we don't mutate the original, then clear non-grouped
			// properties so the response reflects only the aggregation key.
			clone := *ic
			if !aggDims["model_name"] {
				clone.Properties.ModelName = ""
				clone.Properties.ModelVersion = ""
			}
			if !aggDims["namespace"] {
				clone.Properties.Namespace = ""
			}
			if !aggDims["cluster"] {
				clone.Properties.Cluster = ""
			}
			if !aggDims["pod"] {
				clone.Properties.Pod = ""
			}
			if !aggDims["controller"] {
				clone.Properties.Controller = ""
				clone.Properties.ControllerKind = ""
			}
			if !aggDims["controller_kind"] {
				clone.Properties.ControllerKind = ""
			}
			if !aggDims["container"] {
				clone.Properties.Container = ""
			}
			aggMap[key] = &clone
		}
	}

	s.InferenceCosts = aggMap
	return nil
}

// accumulate collapses a slice of InferenceCostSets into a single set whose
// Window spans all of them. It sums additive fields across windows and
// recomputes derived rates. It is used to produce the flat total for the
// /total endpoint.
func accumulate(sets []*InferenceCostSet) *InferenceCostSet {
	if len(sets) == 0 {
		return newInferenceCostSet(opencost.Window{})
	}

	// Determine the combined window.
	first := sets[0]
	combined := opencost.NewClosedWindow(*first.Window.Start(), *first.Window.End())
	for _, s := range sets[1:] {
		if s.Window.Start() != nil && s.Window.Start().Before(*combined.Start()) {
			combined = opencost.NewClosedWindow(*s.Window.Start(), *combined.End())
		}
		if s.Window.End() != nil && s.Window.End().After(*combined.End()) {
			combined = opencost.NewClosedWindow(*combined.Start(), *s.Window.End())
		}
	}

	out := newInferenceCostSet(combined)
	for _, s := range sets {
		for key, ic := range s.InferenceCosts {
			if existing, ok := out.InferenceCosts[key]; ok {
				addResponse(existing, ic)
			} else {
				clone := *ic
				// Update the window to the combined range.
				clone.Window = combined
				out.InferenceCosts[key] = &clone
			}
		}
	}

	return out
}

// --- Phase-1 minimal filter ---

// filterSpec holds a single parsed property:value constraint.
type filterSpec struct {
	property string
	value    string
}

// parseFilter parses a Phase-1 filter string of the form
//   prop:"value"[+prop:"value"]*
// All terms are ANDed. Only dimensions in supportedAggregateProperties are
// accepted. Values are unquoted if surrounded by double-quotes.
//
// This is intentionally minimal for Phase 1. Phase 2 can adopt the full
// core/pkg/filter AST (with wildcard/OR support).
func parseFilter(s string) ([]filterSpec, error) {
	if s == "" {
		return nil, nil
	}
	// "+" is the documented AND separator. HTTP clients that URL-encode query
	// strings may transmit "+" as "%2B" (preserved) or as a literal "+" which
	// some frameworks decode to a space before the handler sees it. Split on
	// both to handle either case.
	s = strings.ReplaceAll(s, " ", "+")
	terms := strings.Split(s, "+")
	specs := make([]filterSpec, 0, len(terms))
	for _, term := range terms {
		term = strings.TrimSpace(term)
		if term == "" {
			continue
		}
		idx := strings.IndexByte(term, ':')
		if idx < 0 {
			return nil, fmt.Errorf("invalid filter term %q: expected property:value", term)
		}
		prop := strings.TrimSpace(term[:idx])
		if !supportedAggregateProperties[prop] {
			return nil, fmt.Errorf("unsupported filter property %q: supported properties are model_name, model_version, namespace, cluster, pod, controller, controller_kind, container", prop)
		}
		val := strings.TrimSpace(term[idx+1:])
		// Strip surrounding double-quotes.
		if len(val) >= 2 && val[0] == '"' && val[len(val)-1] == '"' {
			val = val[1 : len(val)-1]
		}
		specs = append(specs, filterSpec{property: prop, value: val})
	}
	return specs, nil
}

// matchesFilter reports whether an InferenceCostResponse satisfies all filter
// specs (AND semantics).
func matchesFilter(ic *InferenceCostResponse, specs []filterSpec) bool {
	for _, spec := range specs {
		var actual string
		switch spec.property {
		case "model_name":
			actual = ic.Properties.ModelName
		case "model_version":
			actual = ic.Properties.ModelVersion
		case "namespace":
			actual = ic.Properties.Namespace
		case "cluster":
			actual = ic.Properties.Cluster
		case "pod":
			actual = ic.Properties.Pod
		case "controller":
			actual = ic.Properties.Controller
		case "controller_kind":
			actual = ic.Properties.ControllerKind
		case "container":
			actual = ic.Properties.Container
		}
		if actual != spec.value {
			return false
		}
	}
	return true
}
