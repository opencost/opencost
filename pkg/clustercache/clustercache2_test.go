package clustercache

import (
	"testing"

	cc "github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestKubernetesClusterCacheV2GetAllNodesIsolatesLabels(t *testing.T) {
	nodeStore := NewGenericStore(cc.TransformNode)
	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			UID:  types.UID("node-1"),
			Name: "node-1",
			Labels: map[string]string{
				"kubernetes.io/arch": "amd64",
			},
		},
	}
	require.NoError(t, nodeStore.Add(node))

	cache := &KubernetesClusterCacheV2{nodeStore: nodeStore}

	firstRead := cache.GetAllNodes()
	require.Len(t, firstRead, 1)
	firstRead[0].Labels["providerID"] = "aws:///us-east-1a/i-1234567890"

	secondRead := cache.GetAllNodes()
	require.Len(t, secondRead, 1)
	require.NotSame(t, firstRead[0], secondRead[0])
	require.Equal(t, "amd64", secondRead[0].Labels["kubernetes.io/arch"])
	require.NotContains(t, secondRead[0].Labels, "providerID")
}
