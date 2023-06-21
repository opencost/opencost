package kubecost

import (
	"fmt"
	"strings"

	afilter "github.com/opencost/opencost/pkg/filter21/asset"
	"github.com/opencost/opencost/pkg/filter21/ast"
	"github.com/opencost/opencost/pkg/filter21/matcher"
	"github.com/opencost/opencost/pkg/filter21/ops"
	"github.com/opencost/opencost/pkg/filter21/transform"
	"github.com/opencost/opencost/pkg/log"
)

// AssetMatcher is a matcher implementation for Asset instances,
// compiled using the matcher.MatchCompiler.
type AssetMatcher matcher.Matcher[Asset]

// NewAssetMatchCompiler creates a new instance of a
// matcher.MatchCompiler[Asset] which can be used to compile filter.Filter
// ASTs into matcher.Matcher[Asset] implementations.
//
// If the label config is nil, the compiler will fail to compile alias filters
// if any are present in the AST.
//
// If storage interfaces every support querying natively by alias (e.g. if a
// data store contained a "product" attribute on an Asset row), that should
// be handled by a purpose-built AST compiler.
func NewAssetMatchCompiler(labelConfig *LabelConfig) *matcher.MatchCompiler[Asset] {
	passes := []transform.CompilerPass{}

	// The label config pass should be the first pass
	if labelConfig != nil {
		passes = append(passes, NewAssetAliasPass(*labelConfig))
	}

	passes = append(passes,
		transform.PrometheusKeySanitizePass(),
		transform.UnallocatedReplacementPass(),
	)
	return matcher.NewMatchCompiler(
		assetFieldMap,
		assetSliceFieldMap,
		assetMapFieldMap,
		passes...,
	)
}

// Maps fields from an asset to a string value based on an identifier
func assetFieldMap(a Asset, identifier ast.Identifier) (string, error) {
	if identifier.Field == nil {
		return "", fmt.Errorf("cannot map field from identifier with nil field")
	}
	if a == nil {
		return "", fmt.Errorf("cannot map field for nil Asset")
	}

	// Check special fields before defaulting to properties-based fields
	switch afilter.AssetField(identifier.Field.Name) {
	case afilter.FieldType:
		return strings.ToLower(a.Type().String()), nil
	case afilter.FieldLabel:
		labels := a.GetLabels()
		if labels == nil {
			return "", nil
		}
		return labels[identifier.Key], nil
	}

	props := a.GetProperties()
	if props == nil {
		return "", fmt.Errorf("cannot map field for Asset with nil props")
	}

	switch afilter.AssetField(identifier.Field.Name) {
	case afilter.FieldName:
		return props.Name, nil
	case afilter.FieldCategory:
		return props.Category, nil
	case afilter.FieldClusterID:
		return props.Cluster, nil
	case afilter.FieldProject:
		return props.Project, nil
	case afilter.FieldProvider:
		return props.Provider, nil
	case afilter.FieldProviderID:
		return props.ProviderID, nil
	case afilter.FieldAccount:
		return props.Account, nil
	case afilter.FieldService:
		return props.Service, nil
	}

	return "", fmt.Errorf("Failed to find string identifier on Asset: %s", identifier.Field.Name)
}

// Maps slice fields from an asset to a []string value based on an identifier
func assetSliceFieldMap(a Asset, identifier ast.Identifier) ([]string, error) {
	return nil, fmt.Errorf("Assets have no slice fields")
}

// Maps map fields from an Asset to a map[string]string value based on an identifier
func assetMapFieldMap(a Asset, identifier ast.Identifier) (map[string]string, error) {
	if a == nil {
		return nil, fmt.Errorf("cannot get map field for nil Asset")
	}
	switch afilter.AssetField(identifier.Field.Name) {
	case afilter.FieldLabel:
		return a.GetLabels(), nil
	}
	return nil, fmt.Errorf("Failed to find map[string]string identifier on Asset: %s", identifier.Field.Name)
}

// assetAPass implements the transform.CompilerPass interface, providing a pass
// which converts alias nodes to logically-equivalent label/annotation filter
// nodes based on the label config.
type assetAliasPass struct {
	Config              LabelConfig
	AliasNameToAliasKey map[afilter.AssetAlias]string
}

// NewAssetAliasPass creates a compiler pass that converts alias nodes to
// logically-equivalent label/annotation nodes based on the label config.
func NewAssetAliasPass(config LabelConfig) transform.CompilerPass {
	aliasNameToAliasKey := map[afilter.AssetAlias]string{
		// TODO: is external right?
		afilter.DepartmentProp:  config.DepartmentExternalLabel,
		afilter.EnvironmentProp: config.EnvironmentExternalLabel,
		afilter.OwnerProp:       config.OwnerExternalLabel,
		afilter.ProductProp:     config.ProductExternalLabel,
		afilter.TeamProp:        config.TeamExternalLabel,
	}

	return &assetAliasPass{
		Config:              config,
		AliasNameToAliasKey: aliasNameToAliasKey,
	}
}

// Exec implements the transform.CompilerPass interface for an alias pass.
// See aliasPass struct documentation for an explanation.
func (p *assetAliasPass) Exec(filter ast.FilterNode) (ast.FilterNode, error) {
	if p.AliasNameToAliasKey == nil {
		return nil, fmt.Errorf("cannot perform alias conversion with nil mapping of alias name -> key")
	}

	var transformErr error
	leafTransformerFunc := func(node ast.FilterNode) ast.FilterNode {
		if transformErr != nil {
			return node
		}

		var field *ast.Field
		var filterValue string
		var filterOp ast.FilterOp

		switch concrete := node.(type) {
		// These ops are not alias ops, alias ops can only be base-level ops
		// like =, !=, etc. No modification required here.
		case *ast.AndOp, *ast.OrOp, *ast.NotOp, *ast.VoidOp, *ast.ContradictionOp:
			return node

		case *ast.EqualOp:
			field = concrete.Left.Field
			filterValue = concrete.Right
			filterOp = ast.FilterOpEquals
		case *ast.ContainsOp:
			field = concrete.Left.Field
			filterValue = concrete.Right
			filterOp = ast.FilterOpContains
		case *ast.ContainsPrefixOp:
			field = concrete.Left.Field
			filterValue = concrete.Right
			filterOp = ast.FilterOpContainsPrefix
		case *ast.ContainsSuffixOp:
			field = concrete.Left.Field
			filterValue = concrete.Right
			filterOp = ast.FilterOpContainsSuffix
		default:
			transformErr = fmt.Errorf("unknown op '%s' during alias pass", concrete.Op())
			return node
		}
		if field == nil {
			return node
		}
		if !field.IsAlias() {
			return node
		}

		filterFieldAlias := afilter.AssetAlias(field.Name)
		parserAliasKey, ok := p.AliasNameToAliasKey[filterFieldAlias]
		if !ok {
			transformErr = fmt.Errorf("unknown alias field '%s'", filterFieldAlias)
			return node
		}
		labelKey := ops.WithKey(afilter.FieldLabel, parserAliasKey)

		switch filterOp {
		case ast.FilterOpEquals:
			return ops.Eq(labelKey, filterValue)
		case ast.FilterOpContains:
			return ops.Contains(labelKey, filterValue)
		case ast.FilterOpContainsPrefix:
			return ops.ContainsPrefix(labelKey, filterValue)
		case ast.FilterOpContainsSuffix:
			return ops.ContainsSuffix(labelKey, filterValue)
		default:
			transformErr = fmt.Errorf("unexpected failed case match during Asset alias translation, filterOp %s", filterOp)
			log.Errorf("Unexpected failed case match for Asset alias translation, filterOp %s", filterOp)
			return node
		}
	}

	newFilter := ast.TransformLeaves(filter, leafTransformerFunc)

	if transformErr != nil {
		return nil, fmt.Errorf("alias pass transform: %w", transformErr)
	}

	return newFilter, nil
}
