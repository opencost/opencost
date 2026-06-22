package kubemodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestKubeModelSetCodecRoundTrip(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Hour)
	end := start.Add(time.Hour)

	t.Run("empty KubeModelSet", func(t *testing.T) {
		kms := NewKubeModelSet(start, end)

		b, err := kms.MarshalBinary()
		require.NoError(t, err)

		act := new(KubeModelSet)
		err = act.UnmarshalBinary(b)
		require.NoError(t, err)

		KubeModelSetEquals(t, kms, act)
	})

	t.Run("full KubeModelSet", func(t *testing.T) {
		kms := NewMockKubeModelSet(start, end)

		b, err := kms.MarshalBinary()
		require.NoError(t, err)

		act := new(KubeModelSet)
		err = act.UnmarshalBinary(b)
		require.NoError(t, err)

		KubeModelSetEquals(t, kms, act)
	})
}
