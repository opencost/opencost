package opencost

import (
	"fmt"

	"github.com/opencost/opencost/core/pkg/filter/ast"
	"github.com/opencost/opencost/core/pkg/filter/matcher"
	nfilter "github.com/opencost/opencost/core/pkg/filter/node"
	"github.com/opencost/opencost/core/pkg/filter/transform"
)

// NodeMatcher is a matcher implementation for Node instances,
// compiled using the matcher.MatchCompiler.
type NodeMatcher matcher.Matcher[*Node]

// NewNodeMatchCompiler creates a new instance of a
// matcher.MatchCompiler[Node] which can be used to compile filter.Filter
// ASTs into matcher.Matcher[Node] implementations.
//
// If the label config is nil, the compiler will fail to compile alias filters
// if any are present in the AST.
func NewNodeMatchCompiler() *matcher.MatchCompiler[*Node] {
	passes := []transform.CompilerPass{}

	passes = append(passes,
		transform.PrometheusKeySanitizePass(),
		transform.UnallocatedReplacementPass(),
	)
	return matcher.NewMatchCompiler(
		nodeFieldMap,
		nodeSliceFieldMap,
		nodeMapFieldMap,
		passes...,
	)
}

// Maps fields from an asset to a string value based on an identifier
func nodeFieldMap(n *Node, identifier ast.Identifier) (string, error) {
	if identifier.Field == nil {
		return "", fmt.Errorf("cannot map field from identifier with nil field")
	}
	if n == nil {
		return "", fmt.Errorf("cannot map field for nil Node")
	}

	// Check special fields before defaulting to properties-based fields
	switch nfilter.NodeField(identifier.Field.Name) {
	case nfilter.FieldLabel:
		labels := n.GetLabels()
		if labels == nil {
			return "", nil
		}
		return labels[identifier.Key], nil
	}

	props := n.GetProperties()
	if props == nil {
		return "", fmt.Errorf("cannot map field for Node with nil props")
	}

	switch nfilter.NodeField(identifier.Field.Name) {
	case nfilter.FieldName:
		return props.Name, nil
	case nfilter.FieldNodeType:
		return n.NodeType, nil
	case nfilter.FieldClusterID:
		return props.Cluster, nil
	case nfilter.FieldProvider:
		return props.Provider, nil
	case nfilter.FieldProviderID:
		return props.ProviderID, nil
	}

	return "", fmt.Errorf("Failed to find string identifier on Node: %s", identifier.Field.Name)
}

// Maps slice fields from an asset to a []string value based on an identifier
func nodeSliceFieldMap(n *Node, identifier ast.Identifier) ([]string, error) {
	return nil, fmt.Errorf("Nodes have no slice fields")
}

// Maps map fields from an Node to a map[string]string value based on an identifier
func nodeMapFieldMap(n *Node, identifier ast.Identifier) (map[string]string, error) {
	if n == nil {
		return nil, fmt.Errorf("cannot get map field for nil Node")
	}
	switch nfilter.NodeField(identifier.Field.Name) {
	case nfilter.FieldLabel:
		return n.GetLabels(), nil
	}
	return nil, fmt.Errorf("Failed to find map[string]string identifier on Node: %s", identifier.Field.Name)
}
