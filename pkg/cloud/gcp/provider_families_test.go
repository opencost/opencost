package gcp

import (
	"strconv"
	"testing"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func nodeWithMemory(mem string) *clustercache.Node {
	return &clustercache.Node{
		Status: v1.NodeStatus{
			Capacity: v1.ResourceList{v1.ResourceMemory: resource.MustParse(mem)},
		},
	}
}

func TestGetKey_ExtendedMemoryShape(t *testing.T) {
	g := &GCP{}
	labels := map[string]string{
		"node.kubernetes.io/instance-type": "n2d-custom-16-327680-ext",
		"topology.kubernetes.io/region":    "us-west1",
	}
	key := g.GetKey(labels, nodeWithMemory("329990988Ki")).(*gcpKey)
	assert.Equal(t, "n2d", key.extFamily)
	assert.Equal(t, 16, key.extVCPUs)
	assert.Equal(t, 327680, key.extMemMiB)
	assert.Equal(t, int64(329990988*1024), key.capacityBytes)
	// the pricing key itself is unchanged, so parsePage matching still works
	assert.Equal(t, "us-west1,n2dcustom,ondemand", key.Features())

	plain := g.GetKey(map[string]string{
		"node.kubernetes.io/instance-type": "n2d-custom-16-22528",
		"topology.kubernetes.io/region":    "us-west1",
	}, nodeWithMemory("22528Mi")).(*gcpKey)
	assert.Equal(t, 0, plain.extMemMiB)
	assert.Equal(t, "", plain.extFamily)

	// E2 custom shapes cannot carry extended memory: never parsed as such
	e2 := g.GetKey(map[string]string{
		"node.kubernetes.io/instance-type": "e2-custom-4-32768-ext",
		"topology.kubernetes.io/region":    "us-west1",
	}, nil).(*gcpKey)
	assert.Equal(t, 0, e2.extMemMiB)

	// N1 custom shapes are named without the family prefix
	n1 := g.GetKey(map[string]string{
		"node.kubernetes.io/instance-type": "custom-4-32768-ext",
		"topology.kubernetes.io/region":    "us-west1",
	}, nil).(*gcpKey)
	assert.Equal(t, "n1", n1.extFamily)
	assert.Equal(t, 4, n1.extVCPUs)
	assert.Equal(t, 32768, n1.extMemMiB)
	assert.Equal(t, "us-west1,n1custom,ondemand", n1.Features())
}

func TestWithExtendedMemory(t *testing.T) {
	const base, ext = 0.003870, 0.008309 // N2D custom RAM / Extended RAM, us-west1 list
	g := &GCP{Pricing: map[string]*GCPPricing{
		"us-west1,n2dcustom,ondemand":          {Node: &models.Node{VCPUCost: "0.028877", RAMCost: strconv.FormatFloat(base, 'f', -1, 64)}},
		"us-west1,n2dcustom,ondemand,extended": {SKUID: "EXT-SKU", Node: &models.Node{RAMCost: strconv.FormatFloat(ext, 'f', -1, 64)}},
	}}
	region := map[string]string{"topology.kubernetes.io/region": "us-west1"}
	keyFor := func(it string, node *clustercache.Node) models.Key {
		l := map[string]string{"node.kubernetes.io/instance-type": it}
		for k, v := range region {
			l[k] = v
		}
		return g.GetKey(l, node)
	}
	baseNode := g.Pricing["us-west1,n2dcustom,ondemand"].Node

	t.Run("320 GiB on 16 vCPU: 128 standard + 192 extended, blended over reported capacity", func(t *testing.T) {
		key := keyFor("n2d-custom-16-327680-ext", nodeWithMemory("329990988Ki"))
		got := g.withExtendedMemory(key, baseNode)
		require.NotSame(t, baseNode, got, "must return a copy, never mutate the cached pricing")
		capacityGiB := 329990988 * 1024 / float64(1<<30)
		want := (128*base + 192*ext) / capacityGiB
		gotRate, _ := strconv.ParseFloat(got.RAMCost, 64)
		assert.InDelta(t, want, gotRate, 1e-12)
		assert.Equal(t, "0.028877", got.VCPUCost, "CPU untouched")
		// the node total must equal the true GCE RAM charge
		assert.InDelta(t, 128*base+192*ext, gotRate*capacityGiB, 1e-9)
		assert.Equal(t, strconv.FormatFloat(base, 'f', -1, 64), baseNode.RAMCost, "cached base price untouched")
	})

	t.Run("no capacity known: blended over nominal size", func(t *testing.T) {
		key := keyFor("n2d-custom-16-327680-ext", nil)
		gotRate, _ := strconv.ParseFloat(g.withExtendedMemory(key, baseNode).RAMCost, 64)
		assert.InDelta(t, (128*base+192*ext)/320, gotRate, 1e-12)
	})

	t.Run("-ext shape within the family cap bills nothing as extended", func(t *testing.T) {
		key := keyFor("n2d-custom-16-73728-ext", nodeWithMemory("73728Mi")) // 72 GiB < 16*8
		assert.Same(t, baseNode, g.withExtendedMemory(key, baseNode))
	})

	t.Run("N1 cap is 6.5 GiB per vCPU", func(t *testing.T) {
		g.Pricing["us-west1,n1custom,ondemand,extended"] = &GCPPricing{Node: &models.Node{RAMCost: "0.01"}}
		n1 := &models.Node{RAMCost: "0.005"}
		key := keyFor("n1-custom-4-32768-ext", nil) // 32 GiB on 4 vCPU: 26 standard + 6 extended
		gotRate, _ := strconv.ParseFloat(g.withExtendedMemory(key, n1).RAMCost, 64)
		assert.InDelta(t, (26*0.005+6*0.01)/32, gotRate, 1e-12)
	})

	t.Run("no Extended SKU stored: base pricing kept (and warned)", func(t *testing.T) {
		g.Pricing["us-west1,n4dcustom,ondemand"] = &GCPPricing{Node: &models.Node{RAMCost: "0.004"}}
		key := keyFor("n4d-custom-16-327680-ext", nil)
		n := g.Pricing["us-west1,n4dcustom,ondemand"].Node
		assert.Same(t, n, g.withExtendedMemory(key, n))
	})

	t.Run("non-extended node passes through", func(t *testing.T) {
		key := keyFor("n2d-custom-16-22528", nodeWithMemory("22528Mi"))
		assert.Same(t, baseNode, g.withExtendedMemory(key, baseNode))
	})
}
