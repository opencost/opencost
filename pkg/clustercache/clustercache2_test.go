package clustercache

import (
	"sync"
	"testing"
	"time"

	cc "github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/util/promutil"
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
			Annotations: map[string]string{
				"test.annotation": "initial",
			},
		},
	}
	require.NoError(t, nodeStore.Add(node))

	cache := &KubernetesClusterCacheV2{nodeStore: nodeStore}

	firstRead := cache.GetAllNodes()
	require.Len(t, firstRead, 1)
	firstRead[0].Labels["providerID"] = "aws:///us-east-1a/i-1234567890"
	firstRead[0].Labels["kubernetes.io/arch"] = "arm64"
	firstRead[0].Annotations["test.annotation"] = "mutated"
	firstRead[0].Annotations["extra"] = "mutated"

	secondRead := cache.GetAllNodes()
	require.Len(t, secondRead, 1)
	require.NotSame(t, firstRead[0], secondRead[0])
	require.Equal(t, "amd64", secondRead[0].Labels["kubernetes.io/arch"])
	require.NotContains(t, secondRead[0].Labels, "providerID")
	require.Equal(t, "initial", secondRead[0].Annotations["test.annotation"])
	require.NotContains(t, secondRead[0].Annotations, "extra")
}

func TestKubernetesClusterCacheV2GetAllNodesConcurrentSafety(t *testing.T) {
	nodeStore := NewGenericStore(cc.TransformNode)
	for i := 0; i < 20; i++ {
		node := &v1.Node{
			ObjectMeta: metav1.ObjectMeta{
				UID:  types.UID(string(rune('a' + i))),
				Name: string(rune('a' + i)),
				Labels: map[string]string{
					"topology.kubernetes.io/zone": "us-east-1a",
					"node.kubernetes.io/instance": "m5.large",
					"karpenter.sh/capacity-type":  "spot",
					"kubernetes.io/arch":          "amd64",
				},
			},
		}
		require.NoError(t, nodeStore.Add(node))
	}

	cache := &KubernetesClusterCacheV2{nodeStore: nodeStore}

	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	// Readers: simulate collector scrapeNodes running promutil.KubeLabelsToLabelsMap concurrently
	for r := 0; r < 5; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopCh:
					return
				default:
					nodes := cache.GetAllNodes()
					for _, n := range nodes {
						// Mimic collector's promutil.KubeLabelsToLabelsMap
						res := promutil.KubeLabelsToLabelsMap(n.Labels)
						_ = res
					}
				}
			}
		}()
	}

	// Writers: simulate consumers enriching labels or cloud provider GetKey
	for w := 0; w < 5; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopCh:
					return
				default:
					nodes := cache.GetAllNodes()
					for _, n := range nodes {
						n.Labels["providerID"] = "aws:///us-east-1a/i-test"
						n.Labels["mutated"] = "true"
					}
				}
			}
		}()
	}

	// Store updaters: simulate client-go watch events (e.g. Karpenter label changes)
	for u := 0; u < 2; u++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stopCh:
					return
				default:
					updatedNode := &v1.Node{
						ObjectMeta: metav1.ObjectMeta{
							UID:  types.UID("a"),
							Name: "a",
							Labels: map[string]string{
								"topology.kubernetes.io/zone": "us-east-1b",
								"karpenter.sh/capacity-type":  "on-demand",
							},
						},
					}
					_ = nodeStore.Update(updatedNode)
				}
			}
		}(u)
	}

	time.Sleep(200 * time.Millisecond)
	close(stopCh)
	wg.Wait()
}
