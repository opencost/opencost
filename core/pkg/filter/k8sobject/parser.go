package k8sobject

import (
	allocfilter "github.com/opencost/opencost/core/pkg/filter/allocation"
	"github.com/opencost/opencost/core/pkg/filter/ast"
)

// NewK8sObjectFilterParser creates a new `ast.FilterParser` implementation.
//
// It currently just uses Allocation behavior (meaning it supports the same
// fields Allocation does), but we may want to change that later.
func NewK8sObjectFilterParser() ast.FilterParser {
	return allocfilter.NewAllocationFilterParser()
}
