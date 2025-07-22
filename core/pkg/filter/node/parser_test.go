package node

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/filter/ast"
)

func TestDefaultFieldByName(t *testing.T) {
	var nodeField NodeField
	var astf *ast.Field

	nodeField = FieldName
	astf = DefaultFieldByName(nodeField)
	if astf.Name != "name" {
		t.Errorf("expected %s; received %s", "name", astf.Name)
	}

	nodeField = FieldNodeType
	astf = DefaultFieldByName(nodeField)
	if astf.Name != "nodeType" {
		t.Errorf("expected %s; received %s", "nodeType", astf.Name)
	}

	nodeField = FieldClusterID
	astf = DefaultFieldByName(nodeField)
	if astf.Name != "cluster" {
		t.Errorf("expected %s; received %s", "cluster", astf.Name)
	}

	nodeField = FieldProvider
	astf = DefaultFieldByName(nodeField)
	if astf.Name != "provider" {
		t.Errorf("expected %s; received %s", "provider", astf.Name)
	}

	nodeField = FieldProviderID
	astf = DefaultFieldByName(nodeField)
	if astf.Name != "providerID" {
		t.Errorf("expected %s; received %s", "providerID", astf.Name)
	}

	nodeField = FieldLabel
	astf = DefaultFieldByName(nodeField)
	if astf.Name != "label" {
		t.Errorf("expected %s; received %s", "label", astf.Name)
	}
}
