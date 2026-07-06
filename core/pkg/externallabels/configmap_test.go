package externallabels

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestNewConfigMapProvider_NilClient(t *testing.T) {
	_, err := NewConfigMapProvider(nil, "default")
	assert.Error(t, err)
}

func TestConfigMapProvider_Labels(t *testing.T) {
	cm := &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "external-labels",
			Namespace: "default",
			Labels: map[string]string{
				ExternalLabelsLabelKey: ExternalLabelsLabelValue,
			},
		},
		Data: map[string]string{
			"cluster": "1de3e77b-266d-48c1-91cb-ec5e22902af7",
			"env":     "dev",
			"region":  "nam",
		},
	}

	client := fake.NewClientset(cm)
	p, err := NewConfigMapProvider(client, "default")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	go func() {
		_ = p.Start(ctx)
	}()

	// Give the informer a moment to sync.
	require.Eventually(t, func() bool {
		labels, err := p.Labels(context.Background())
		return err == nil && len(labels) == 3
	}, 2*time.Second, 50*time.Millisecond)

	labels, err := p.Labels(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "1de3e77b-266d-48c1-91cb-ec5e22902af7", labels["cluster"])
	assert.Equal(t, "dev", labels["env"])
	assert.Equal(t, "nam", labels["region"])
}

func TestConfigMapProvider_NoMatchingConfigMaps(t *testing.T) {
	// ConfigMap without the required label — should not be picked up.
	cm := &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "unrelated",
			Namespace: "default",
		},
		Data: map[string]string{"key": "value"},
	}

	client := fake.NewClientset(cm)
	p, err := NewConfigMapProvider(client, "default")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	go func() {
		_ = p.Start(ctx)
	}()

	// Wait for informer to have synced (small sleep is sufficient with fake client).
	time.Sleep(200 * time.Millisecond)

	labels, err := p.Labels(context.Background())
	require.NoError(t, err)
	assert.Empty(t, labels)
}
