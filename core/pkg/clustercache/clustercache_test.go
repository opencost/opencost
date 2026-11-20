package clustercache

import (
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestTransformNodeIsolatesMaps(t *testing.T) {
	v1Node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			UID:  types.UID("node-1"),
			Name: "node-1",
			Labels: map[string]string{
				"kubernetes.io/arch": "amd64",
			},
			Annotations: map[string]string{
				"cluster.k8s.io/test": "true",
			},
		},
		Spec: v1.NodeSpec{
			ProviderID: "aws:///us-east-1a/i-12345",
		},
	}

	transformed := TransformNode(v1Node)
	if transformed == nil {
		t.Fatal("expected non-nil transformed Node")
	}

	// Mutate source node maps
	v1Node.Labels["kubernetes.io/arch"] = "arm64"
	v1Node.Labels["new-label"] = "val"
	v1Node.Annotations["cluster.k8s.io/test"] = "false"
	v1Node.Annotations["new-annotation"] = "val"

	if transformed.Labels["kubernetes.io/arch"] != "amd64" {
		t.Errorf("expected transformed.Labels to be amd64, got %s", transformed.Labels["kubernetes.io/arch"])
	}
	if _, ok := transformed.Labels["new-label"]; ok {
		t.Errorf("transformed.Labels leaked new-label from source")
	}
	if transformed.Annotations["cluster.k8s.io/test"] != "true" {
		t.Errorf("expected transformed.Annotations to be true, got %s", transformed.Annotations["cluster.k8s.io/test"])
	}
	if _, ok := transformed.Annotations["new-annotation"]; ok {
		t.Errorf("transformed.Annotations leaked new-annotation from source")
	}

	// Mutate transformed node maps
	transformed.Labels["transformed-key"] = "x"
	transformed.Annotations["transformed-key"] = "y"

	if _, ok := v1Node.Labels["transformed-key"]; ok {
		t.Errorf("source v1Node leaked transformed-key in labels")
	}
	if _, ok := v1Node.Annotations["transformed-key"]; ok {
		t.Errorf("source v1Node leaked transformed-key in annotations")
	}
}

func TestNodeClone(t *testing.T) {
	var nilNode *Node
	if nilNode.Clone() != nil {
		t.Fatal("expected nil clone for nil node")
	}

	node := &Node{
		UID:  types.UID("node-1"),
		Name: "node-1",
		Labels: map[string]string{
			"env": "prod",
		},
		Annotations: map[string]string{
			"team": "core",
		},
		SpecProviderID: "aws:///us-east-1a/i-12345",
	}

	clone := node.Clone()
	if clone == nil {
		t.Fatal("expected non-nil clone")
	}
	if clone == node {
		t.Fatal("expected distinct node pointer")
	}

	clone.Labels["env"] = "staging"
	clone.Labels["extra"] = "val"
	clone.Annotations["team"] = "infra"
	clone.Annotations["extra"] = "val"

	if node.Labels["env"] != "prod" {
		t.Errorf("expected original env to remain prod, got %s", node.Labels["env"])
	}
	if _, ok := node.Labels["extra"]; ok {
		t.Errorf("original labels leaked extra from clone")
	}
	if node.Annotations["team"] != "core" {
		t.Errorf("expected original team to remain core, got %s", node.Annotations["team"])
	}
	if _, ok := node.Annotations["extra"]; ok {
		t.Errorf("original annotations leaked extra from clone")
	}
}
