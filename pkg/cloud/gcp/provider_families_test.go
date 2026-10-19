package gcp

import (
	"bytes"
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

	t.Run("GPU node: the ,gpu key suffix is dropped when looking up the Extended SKU", func(t *testing.T) {
		g.Pricing["us-west1,n1custom,ondemand,extended"] = &GCPPricing{Node: &models.Node{RAMCost: "0.01"}}
		key := g.GetKey(map[string]string{
			"node.kubernetes.io/instance-type": "n1-custom-4-32768-ext",
			"topology.kubernetes.io/region":    "us-west1",
			GKE_GPU_TAG:                        "nvidia-tesla-t4",
		}, nil)
		require.Equal(t, "us-west1,n1custom,ondemand,gpu", key.Features())
		n1 := &models.Node{RAMCost: "0.005", GPUCost: "0.35"}
		got := g.withExtendedMemory(key, n1)
		gotRate, _ := strconv.ParseFloat(got.RAMCost, 64)
		assert.InDelta(t, (26*0.005+6*0.01)/32, gotRate, 1e-12)
		assert.Equal(t, "0.35", got.GPUCost, "GPU price untouched")
	})

	t.Run("non-extended node passes through", func(t *testing.T) {
		key := keyFor("n2d-custom-16-22528", nodeWithMemory("22528Mi"))
		assert.Same(t, baseNode, g.withExtendedMemory(key, baseNode))
	})
}

func regionKey(region, it string) *gcpKey {
	return &gcpKey{Labels: map[string]string{
		"node.kubernetes.io/instance-type": it,
		"topology.kubernetes.io/region":    region,
	}}
}

func spotKey(region, it string) *gcpKey {
	k := regionKey(region, it)
	k.Labels["cloud.google.com/gke-spot"] = "true"
	return k
}

// C4D RAM is priced per decimal GB (GBy.h) while most families are priced per
// GiB (GiBy.h); both must come out as a per-GiB rate.
func TestParsePage_DecimalGBRamSKU(t *testing.T) {
	// factor == 0 omits baseUnit/baseUnitConversionFactor, exercising the
	// usageUnit-string fallback; otherwise the byte-second factor decides.
	sku := func(id, desc, group, usage, unit string, factor int64, nanos int) string {
		conv := ""
		if factor > 0 {
			conv = `"baseUnit":"By.s","baseUnitConversionFactor":` + strconv.FormatInt(factor, 10) + `,`
		}
		return `{"name":"services/x/skus/` + id + `","skuId":"` + id + `","description":"` + desc + `",
		 "category":{"serviceDisplayName":"Compute Engine","resourceFamily":"Compute","resourceGroup":"` + group + `","usageType":"` + usage + `"},
		 "serviceRegions":["us-east1"],
		 "pricingInfo":[{"pricingExpression":{` + conv + `"usageUnit":"` + unit + `","tieredRates":[{"unitPrice":{"currencyCode":"USD","units":"0","nanos":` + strconv.Itoa(nanos) + `}}]}}]}`
	}
	page := `{"skus":[` +
		sku("C4D-CORE-OD", "C4D Instance Core running in Americas", "CPU", "OnDemand", "h", 0, 32704000) + "," +
		sku("C4D-RAM-OD", "C4D Instance Ram running in Americas", "RAM", "OnDemand", "GBy.h", 3600000000000, 3496000) + "," + // factor path
		sku("C4D-CORE-SP", "Spot Preemptible C4D Instance Core running in Americas", "CPU", "Preemptible", "h", 0, 13860000) + "," +
		sku("C4D-RAM-SP", "Spot Preemptible C4D Instance Ram running in Americas", "RAM", "Preemptible", "GBy.h", 0, 1484000) + "," + // string fallback
		sku("N2D-CORE-SP", "Spot Preemptible N2D AMD Instance Core running in Americas", "CPU", "Preemptible", "h", 0, 14010000) + "," +
		sku("N2D-RAM-SP", "Spot Preemptible N2D AMD Instance Ram running in Americas", "RAM", "Preemptible", "GiBy.h", 3865470566400, 1876000) + // binary, must be identity
		`],"nextPageToken":""}`

	inputKeys := map[string]models.Key{}
	for _, it := range []string{"c4d-highcpu-16", "n2d-highcpu-16"} {
		od := regionKey("us-east1", it)
		sp := spotKey("us-east1", it)
		inputKeys[od.Features()] = od
		inputKeys[sp.Features()] = sp
	}
	g := &GCP{}
	prices, _, err := g.parsePage(bytes.NewReader([]byte(page)), inputKeys, nil)
	require.NoError(t, err)

	rate := func(key, field string) float64 {
		p, ok := prices[key]
		require.True(t, ok, "missing key %s", key)
		s := p.Node.RAMCost
		if field == "cpu" {
			s = p.Node.VCPUCost
		}
		f, err := strconv.ParseFloat(s, 64)
		require.NoError(t, err, "%s %s = %q", key, field, s)
		return f
	}
	// GBy.h -> per GiB
	assert.InDelta(t, 0.003496*gibPerGB, rate("us-east1,c4dstandard,ondemand", "ram"), 1e-9)
	assert.InDelta(t, 0.001484*gibPerGB, rate("us-east1,c4dstandard,preemptible", "ram"), 1e-9)
	// core rates are per hour and must not be touched
	assert.InDelta(t, 0.032704, rate("us-east1,c4dstandard,ondemand", "cpu"), 1e-9)
	assert.InDelta(t, 0.013860, rate("us-east1,c4dstandard,preemptible", "cpu"), 1e-9)
	// GiBy.h passes through unchanged
	assert.InDelta(t, 0.001876, rate("us-east1,n2dstandard,preemptible", "ram"), 1e-9)
	assert.InDelta(t, 0.014010, rate("us-east1,n2dstandard,preemptible", "cpu"), 1e-9)
	// the constant itself: 2^30 / 10^9
	assert.InDelta(t, 1.073741824, gibPerGB, 1e-12)
}
