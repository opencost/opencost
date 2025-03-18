package opencost

import (
	"fmt"

	ast "github.com/opencost/opencost/core/pkg/filter/ast"
	"github.com/opencost/opencost/core/pkg/filter/matcher"
	nfilter "github.com/opencost/opencost/core/pkg/filter/networkinsight"
	"github.com/opencost/opencost/core/pkg/filter/transform"
)

// NetworkInsightMatcher is a matcher implementation for NetworkInsightSet,
// compiled using the matcher.MatchCompiler.
type NetworkInsightMatcher matcher.Matcher[*NetworkInsight]

// NewNetworkInsightMatchCompiler creates a new instance of a
// matcher.MatchCompiler[NetworkInsight] which can be used to compile filter.Filter
// ASTs into matcher.Matcher[NetworkInsight] implementations.
//
// If the label config is nil, the compiler will fail to compile alias filters
// if any are present in the AST.
//
// If storage interfaces every support querying natively by alias (e.g. if a
// data store contained a "product" attribute on an Asset row), that should
// be handled by a purpose-built AST compiler.

func NewNetworkInsightMatchCompiler() *matcher.MatchCompiler[*NetworkInsight] {
	passes := []transform.CompilerPass{}

	return matcher.NewMatchCompiler(
		networkInsightFieldMap,
		networkInsightSliceFieldMap,
		networkInsightMapFieldMap,
		passes...,
	)
}

// Maps fields from a network insight to a string value based on an identifier
func networkInsightFieldMap(ni *NetworkInsight, identifier ast.Identifier) (string, error) {
	if ni == nil {
		return "", fmt.Errorf("cannot map field for nil Network insight")
	}

	if identifier.Field == nil {
		return "", fmt.Errorf("cannot map field from identifier with nil field")
	}

	switch nfilter.NetworkInsightField(identifier.Field.Name) {
	case nfilter.FieldClusterID:
		return ni.Cluster, nil
	case nfilter.FieldNamespace:
		return ni.Namespace, nil
	case nfilter.FieldPod:
		return ni.Pod, nil
	}

	return "", fmt.Errorf("Failed to find string identifier on Network Insight: %s", identifier.Field.Name)
}

// Maps slice fields from a network insight to a []string value based on an identifier
func networkInsightSliceFieldMap(ni *NetworkInsight, identifier ast.Identifier) ([]string, error) {
	return nil, fmt.Errorf("NetworkInsights have no slice fields")
}

// Maps map fields from a network insight to a map[string]string value based on an identifier
func networkInsightMapFieldMap(ni *NetworkInsight, identifier ast.Identifier) (map[string]string, error) {
	return nil, fmt.Errorf("NetworkInsights have no map fields")
}

// NetworkInsightMatcher is a matcher implementation for NetworkInsightSet,
// compiled using the matcher.MatchCompiler.
type NetworkInsightDetailMatcher matcher.Matcher[*NetworkDetail]

// NewNetworkInsightMatchCompiler creates a new instance of a
// matcher.MatchCompiler[NetworkInsight] which can be used to compile filter.Filter
// ASTs into matcher.Matcher[NetworkInsight] implementations.
//
// If the label config is nil, the compiler will fail to compile alias filters
// if any are present in the AST.
//
// If storage interfaces every support querying natively by alias (e.g. if a
// data store contained a "product" attribute on an Asset row), that should
// be handled by a purpose-built AST compiler.

func NewNetworkInsightDetailMatchCompiler() *matcher.MatchCompiler[*NetworkDetail] {
	passes := []transform.CompilerPass{}

	return matcher.NewMatchCompiler(
		networkInsightDetailFieldMap,
		networkInsightDetailSliceFieldMap,
		networkInsightDetailMapFieldMap,
		passes...,
	)
}

// Maps fields from a network insight to a string value based on an identifier
func networkInsightDetailFieldMap(nd *NetworkDetail, identifier ast.Identifier) (string, error) {
	if nd == nil {
		return "", fmt.Errorf("cannot map field for nil Network insight")
	}

	if identifier.Field == nil {
		return "", fmt.Errorf("cannot map field from identifier with nil field")
	}

	switch nfilter.NetworkInsightDetailField(identifier.Field.Name) {
	case nfilter.FieldEndPoint:
		return nd.EndPoint, nil
	}
	return "", fmt.Errorf("Failed to find string identifier on Network Insight: %s", identifier.Field.Name)
}

// Maps slice fields from a network insight to a []string value based on an identifier
func networkInsightDetailSliceFieldMap(nd *NetworkDetail, identifier ast.Identifier) ([]string, error) {
	return nil, fmt.Errorf("NetworkInsights have no slice fields")
}

// Maps map fields from a network insight to a map[string]string value based on an identifier
func networkInsightDetailMapFieldMap(nd *NetworkDetail, identifier ast.Identifier) (map[string]string, error) {
	return nil, fmt.Errorf("NetworkInsights have no map fields")
}
