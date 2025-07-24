package opencost

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/filter/ast"
	"github.com/opencost/opencost/core/pkg/filter/node"
)

func TestNodeMatcher(t *testing.T) {
	var n *Node
	var id ast.Identifier
	var act string
	var actMap map[string]string
	var err error

	n = &Node{
		Properties: &AssetProperties{
			Cluster:    "cluster",
			ProviderID: "providerid",
			Provider:   "provider",
			Name:       "name",
		},
		Labels: AssetLabels{
			"nodegroup": "ng",
			"os":        "linux",
		},
		NodeType: "nodetype",
	}

	// test nodeFieldMap
	id = ast.Identifier{
		Field: &ast.Field{Name: string(node.FieldProviderID)},
	}
	act, err = nodeFieldMap(nil, id)
	if act != "" || err == nil {
		t.Errorf("expected error for nil node")
	}

	act, err = nodeFieldMap(n, id)
	if err != nil {
		t.Errorf("unexpected error for non-nil node")
	}
	if act != "providerid" {
		t.Errorf("expected %s; received %s", "providerid", act)
	}

	id = ast.Identifier{
		Field: &ast.Field{Name: string(node.FieldClusterID)},
	}
	act, err = nodeFieldMap(nil, id)
	if act != "" || err == nil {
		t.Errorf("expected error for nil node")
	}
	act, err = nodeFieldMap(n, id)
	if err != nil {
		t.Errorf("unexpected error for non-nil node")
	}
	if act != "cluster" {
		t.Errorf("expected %s; received %s", "cluster", act)
	}

	id = ast.Identifier{
		Field: &ast.Field{Name: string(node.FieldName)},
	}
	act, err = nodeFieldMap(nil, id)
	if act != "" || err == nil {
		t.Errorf("expected error for nil node")
	}
	act, err = nodeFieldMap(n, id)
	if err != nil {
		t.Errorf("unexpected error for non-nil node")
	}
	if act != "name" {
		t.Errorf("expected %s; received %s", "name", act)
	}

	id = ast.Identifier{
		Field: &ast.Field{Name: string(node.FieldNodeType)},
	}
	act, err = nodeFieldMap(nil, id)
	if act != "" || err == nil {
		t.Errorf("expected error for nil node")
	}
	act, err = nodeFieldMap(n, id)
	if err != nil {
		t.Errorf("unexpected error for non-nil node")
	}
	if act != "nodetype" {
		t.Errorf("expected %s; received %s", "nodetype", act)
	}

	id = ast.Identifier{
		Field: &ast.Field{Name: string(node.FieldProvider)},
	}
	act, err = nodeFieldMap(nil, id)
	if act != "" || err == nil {
		t.Errorf("expected error for nil node")
	}
	act, err = nodeFieldMap(n, id)
	if err != nil {
		t.Errorf("unexpected error for non-nil node")
	}
	if act != "provider" {
		t.Errorf("expected %s; received %s", "provider", act)
	}

	id = ast.Identifier{
		Field: &ast.Field{Name: string(node.FieldLabel)},
		Key:   "nodegroup",
	}
	act, err = nodeFieldMap(nil, id)
	if act != "" || err == nil {
		t.Errorf("expected error for nil node")
	}
	act, err = nodeFieldMap(n, id)
	if err != nil {
		t.Errorf("unexpected error for non-nil node")
	}
	if act != "ng" {
		t.Errorf("expected %s; received %s", "ng", act)
	}

	// test nodeSliceFieldMap
	id = ast.Identifier{}
	_, err = nodeSliceFieldMap(nil, id)
	if err == nil {
		t.Errorf("expected error for slice")
	}
	_, err = nodeSliceFieldMap(n, id)
	if err == nil {
		t.Errorf("expected error for slice")
	}

	// test nodeMapFieldMap
	id = ast.Identifier{
		Field: &ast.Field{Name: string(node.FieldLabel)},
	}
	actMap, err = nodeMapFieldMap(nil, id)
	if err == nil {
		t.Errorf("expected error for nil node")
	}
	actMap, err = nodeMapFieldMap(n, id)
	if len(actMap) != 2 || err != nil {
		t.Errorf("unexpected error for map")
	}
}
