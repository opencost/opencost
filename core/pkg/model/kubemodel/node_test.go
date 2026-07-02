package kubemodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestValidateNode(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Hour)
	end := start.Add(time.Hour)
	window := Window{Start: start, End: end}

	tests := []struct {
		name    string
		node    *Node
		wantErr string
	}{
		{
			name:    "empty UID",
			node:    &Node{Name: "my-node", Start: start, End: end},
			wantErr: "UID is missing for Node with name 'my-node'",
		},
		{
			name:    "empty Name",
			node:    &Node{UID: "node-uid", Start: start, End: end},
			wantErr: "Name is missing for Node 'node-uid'",
		},
		{
			name:    "outside window",
			node:    &Node{UID: "node-uid", Name: "my-node", Start: start.Add(-time.Hour), End: end},
			wantErr: checkWindow(window, start.Add(-time.Hour), end).Error(),
		},
		{
			name: "valid",
			node: &Node{UID: "node-uid", Name: "my-node", Start: start, End: end},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.node.ValidateNode(window)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRegisterNode(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Hour)
	end := start.Add(time.Hour)

	newNode := func(uid, name string) *Node {
		return &Node{UID: uid, Name: name, Start: start, End: end}
	}
	withCluster := func(kms *KubeModelSet) {
		kms.RegisterCluster(&Cluster{UID: "cluster-uid", Start: start, End: end})
	}

	tests := []struct {
		name    string
		setup   func(*KubeModelSet)
		node    *Node
		wantErr string
		want    *KubeModelSet
	}{
		{
			name:    "validation failure",
			node:    &Node{UID: "", Name: "my-node", Start: start, End: end},
			wantErr: "RegisterNode: invalid node: UID is missing for Node with name 'my-node'",
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				kms.Metadata.Diagnostics = []Diagnostic{
					{Level: DiagnosticLevelError, Message: "RegisterNode: invalid node: UID is missing for Node with name 'my-node'"},
				}
				return kms
			}(),
		},
		{
			name: "warns when cluster is nil",
			node: newNode("node-uid", "my-node"),
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				kms.Nodes["node-uid"] = newNode("node-uid", "my-node")
				kms.Metadata.ObjectCount = 1
				kms.Metadata.Diagnostics = []Diagnostic{
					{Level: DiagnosticLevelWarning, Message: "RegisterNode: Cluster is nil"},
				}
				return kms
			}(),
		},
		{
			name:  "registers node with cluster",
			setup: withCluster,
			node:  newNode("node-uid", "my-node"),
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				withCluster(kms)
				kms.Nodes["node-uid"] = newNode("node-uid", "my-node")
				kms.Metadata.ObjectCount = 1
				return kms
			}(),
		},
		{
			name: "duplicate registration is a no-op",
			setup: func(kms *KubeModelSet) {
				withCluster(kms)
				kms.RegisterNode(newNode("node-uid", "original"))
			},
			node: newNode("node-uid", "duplicate"),
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				withCluster(kms)
				kms.Nodes["node-uid"] = newNode("node-uid", "original")
				kms.Metadata.ObjectCount = 1
				return kms
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kms := NewKubeModelSet(start, end)
			if tt.setup != nil {
				tt.setup(kms)
			}

			err := kms.RegisterNode(tt.node)

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}

			KubeModelSetEquals(t, tt.want, kms)
		})
	}
}
