package kubecost

import (
	"fmt"

	allocationfilter "github.com/opencost/opencost/pkg/filter21/allocation"
	"github.com/opencost/opencost/pkg/filter21/ast"
	"github.com/opencost/opencost/pkg/filter21/matcher"
	"github.com/opencost/opencost/pkg/filter21/transform"
)

// AllocationMatcher is a matcher implementation for Allocation instances,
// compiled using the matcher.MatchCompiler for allocations.
type AllocationMatcher matcher.Matcher[*Allocation]

// NewAllocationMatchCompiler creates a new instance of a matcher.MatchCompiler[*Allocation]
// which can be used to compile filter.Filter ASTs into matcher.Matcher[*Allocation]
// implementations.
func NewAllocationMatchCompiler() *matcher.MatchCompiler[*Allocation] {
	return matcher.NewMatchCompiler(
		allocationFieldMap,
		allocationSliceFieldMap,
		allocationMapFieldMap,
		transform.PrometheusKeySanitizePass(),
		transform.UnallocatedReplacementPass())
}

// Maps fields from an allocation to a string value based on an identifier
func allocationFieldMap(a *Allocation, identifier ast.Identifier) (string, error) {
	switch allocationfilter.AllocationField(identifier.Field.Name) {
	case allocationfilter.AllocationFieldNamespace:
		return a.Properties.Namespace, nil
	case allocationfilter.AllocationFieldNode:
		return a.Properties.Node, nil
	case allocationfilter.AllocationFieldClusterID:
		return a.Properties.Cluster, nil
	case allocationfilter.AllocationFieldControllerName:
		return a.Properties.Controller, nil
	case allocationfilter.AllocationFieldControllerKind:
		return a.Properties.ControllerKind, nil
	case allocationfilter.AllocationFieldPod:
		return a.Properties.Pod, nil
	case allocationfilter.AllocationFieldContainer:
		return a.Properties.Container, nil
	case allocationfilter.AllocationFieldProvider:
		return a.Properties.ProviderID, nil
	case allocationfilter.AllocationFieldLabel:
		return a.Properties.Labels[identifier.Key], nil
	case allocationfilter.AllocationFieldAnnotation:
		return a.Properties.Annotations[identifier.Key], nil
	}

	return "", fmt.Errorf("Failed to find string identifier on Allocation: %s", identifier.Field.Name)
}

// Maps slice fields from an allocation to a []string value based on an identifier
func allocationSliceFieldMap(a *Allocation, identifier ast.Identifier) ([]string, error) {
	switch allocationfilter.AllocationField(identifier.Field.Name) {
	case allocationfilter.AllocationFieldServices:
		return a.Properties.Services, nil
	}

	return nil, fmt.Errorf("Failed to find []string identifier on Allocation: %s", identifier.Field.Name)
}

// Maps map fields from an allocation to a map[string]string value based on an identifier
func allocationMapFieldMap(a *Allocation, identifier ast.Identifier) (map[string]string, error) {
	switch allocationfilter.AllocationField(identifier.Field.Name) {
	case allocationfilter.AllocationFieldLabel:
		return a.Properties.Labels, nil
	case allocationfilter.AllocationFieldAnnotation:
		return a.Properties.Annotations, nil
	}
	return nil, fmt.Errorf("Failed to find map[string]string identifier on Allocation: %s", identifier.Field.Name)
}
