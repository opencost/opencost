package clustercache

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	cc "github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestGenericStoreReplaceAtomic(t *testing.T) {
	var startOnce sync.Once
	transformStarted := make(chan struct{})
	continueTransform := make(chan struct{})

	// Custom transform that pauses while transforming replacement items to allow
	// deterministic observation of intermediate state.
	nodeStore := NewGenericStore(func(input *v1.Node) *cc.Node {
		transformed := cc.TransformNode(input)
		if input.Name == "new-0" {
			startOnce.Do(func() {
				close(transformStarted)
			})
			<-continueTransform
		}
		return transformed
	})

	// Populate initial 10 nodes ("old-0" to "old-9")
	for i := 0; i < 10; i++ {
		name := fmt.Sprintf("old-%d", i)
		node := &v1.Node{
			ObjectMeta: metav1.ObjectMeta{
				UID:  types.UID(name),
				Name: name,
				Labels: map[string]string{
					"key": "val",
				},
			},
		}
		require.NoError(t, nodeStore.Add(node))
	}

	initialItems := nodeStore.GetAll()
	require.Len(t, initialItems, 10)
	for _, item := range initialItems {
		require.True(t, strings.HasPrefix(item.Name, "old-"))
	}

	// Prepare 20 replacement nodes ("new-0" to "new-19")
	newList := make([]any, 20)
	for i := 0; i < 20; i++ {
		name := fmt.Sprintf("new-%d", i)
		newList[i] = &v1.Node{
			ObjectMeta: metav1.ObjectMeta{
				UID:  types.UID(name),
				Name: name,
				Labels: map[string]string{
					"new": "val",
				},
			},
		}
	}

	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	// Concurrently read while Replace runs: readers must only ever see complete
	// old snapshot (10 items) or complete new snapshot (20 items), never partial.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopCh:
				return
			default:
				items := nodeStore.GetAll()
				if len(items) != 10 && len(items) != 20 {
					t.Errorf("GetAll returned partial snapshot of %d items (expected 10 or 20)", len(items))
				}
			}
		}
	}()

	// Start Replace in background
	replaceDone := make(chan error, 1)
	go func() {
		replaceDone <- nodeStore.Replace(newList, "")
	}()

	// Wait until Replace is actively transforming new items
	<-transformStarted

	// While Replace is paused midway, the store must still serve the complete old snapshot
	midReplaceItems := nodeStore.GetAll()
	require.Len(t, midReplaceItems, 10)
	for _, item := range midReplaceItems {
		require.True(t, strings.HasPrefix(item.Name, "old-"))
	}

	// Unblock transform to allow Replace to finish publishing new items atomically
	close(continueTransform)

	// Wait for Replace to complete
	require.NoError(t, <-replaceDone)

	// Stop concurrent reader
	close(stopCh)
	wg.Wait()

	// Final verification: store now serves the complete new snapshot
	finalItems := nodeStore.GetAll()
	require.Len(t, finalItems, 20)
	for _, item := range finalItems {
		require.True(t, strings.HasPrefix(item.Name, "new-"))
	}
}
