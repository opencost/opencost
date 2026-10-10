package carbon

import (
	"fmt"
	"sync"
	"testing"

	"github.com/opencost/opencost/core/pkg/opencost"
)

// TestRegistry_NodeCoeff_WinsForProviderWithoutStaticRows verifies that a
// provider with no static embedded rows (Scaleway) resolves node coefficients
// from the runtime registry.
func TestRegistry_NodeCoeff_WinsForProviderWithoutStaticRows(t *testing.T) {
	// Scaleway has no rows in the static table — the registry is its source.
	if _, ok := staticNodeProviders[opencost.ScalewayProvider]; ok {
		t.Fatal("test precondition violated: Scaleway must not have static node rows")
	}

	coeff := 5.840293e-7 // t/h (reference value, 2026-09-24)
	RegisterRuntimeNodeCoefficients(opencost.ScalewayProvider, map[NodeCoefficient]float64{
		{Region: "fr-par", InstanceType: "BASIC3-X2C-4G"}: coeff,
	})
	t.Cleanup(func() { RegisterRuntimeNodeCoefficients(opencost.ScalewayProvider, nil) })

	n := nodeWithLabels(opencost.ScalewayProvider, "scaleway://instance/scw-1234", "fr-par", "BASIC3-X2C-4G", 60)
	if got := lookupCarbonCoeff(n); !floatEqual(got, coeff) {
		t.Fatalf("lookupCarbonCoeff = %g, want %g (registry value)", got, coeff)
	}
}

// TestRegistry_MissFallsThroughToZero verifies that a registry miss for a
// provider without static rows keeps the existing zero behavior.
func TestRegistry_MissFallsThroughToZero(t *testing.T) {
	RegisterRuntimeNodeCoefficients(opencost.ScalewayProvider, map[NodeCoefficient]float64{
		{Region: "fr-par", InstanceType: "BASIC3-X2C-4G"}: 5.840293e-7,
	})
	t.Cleanup(func() { RegisterRuntimeNodeCoefficients(opencost.ScalewayProvider, nil) })

	// Different region → miss → 0.
	n := nodeWithLabels(opencost.ScalewayProvider, "scaleway://instance/scw-1234", "nl-ams", "BASIC3-X2C-4G", 60)
	if got := lookupCarbonCoeff(n); got != 0 {
		t.Fatalf("lookupCarbonCoeff = %g, want 0 for region miss", got)
	}
	// Different instance type → miss → 0.
	n = nodeWithLabels(opencost.ScalewayProvider, "scaleway://instance/scw-1234", "fr-par", "OTHER-TYPE", 60)
	if got := lookupCarbonCoeff(n); got != 0 {
		t.Fatalf("lookupCarbonCoeff = %g, want 0 for instance-type miss", got)
	}
}

// TestRegistry_StaticProvidersUnaffected verifies that providers with static
// rows (AWS/GCP/Azure) are never served from the registry: the static table
// keeps precedence.
func TestRegistry_StaticProvidersUnaffected(t *testing.T) {
	staticCoeff, ok := carbonLookupNode[carbonLookupKeyNode{opencost.AWSProvider, "us-east-1", "t4g.nano"}]
	if !ok {
		t.Fatal("test precondition violated: AWS/us-east-1/t4g.nano must be in the static table")
	}

	// Register a conflicting value for AWS — it must be ignored.
	RegisterRuntimeNodeCoefficients(opencost.AWSProvider, map[NodeCoefficient]float64{
		{Region: "us-east-1", InstanceType: "t4g.nano"}: 999.0,
	})
	t.Cleanup(func() { RegisterRuntimeNodeCoefficients(opencost.AWSProvider, nil) })

	n := nodeWithLabels(opencost.AWSProvider, "aws:///us-east-1a/i-1", "us-east-1", "t4g.nano", 60)
	if got := lookupCarbonCoeff(n); !floatEqual(got, staticCoeff) {
		t.Fatalf("lookupCarbonCoeff = %g, want static %g (registry must not override)", got, staticCoeff)
	}
}

// TestRegistry_WholeSetReplacement verifies atomic whole-set replacement:
// registering a new set removes entries from the previous set, and clearing
// (nil) removes the provider entirely — a failed refresh that does not
// re-register keeps the prior coefficients (data-model.md §5).
func TestRegistry_WholeSetReplacement(t *testing.T) {
	RegisterRuntimeNodeCoefficients("TestProvider", map[NodeCoefficient]float64{
		{Region: "r1", InstanceType: "a"}: 1.0,
		{Region: "r2", InstanceType: "b"}: 2.0,
	})
	t.Cleanup(func() { RegisterRuntimeNodeCoefficients("TestProvider", nil) })

	if _, ok := runtimeNodeCoefficient("TestProvider", "r1", "a"); !ok {
		t.Fatal("first set entry missing")
	}

	// Replace with a different set: the old entry must be gone.
	RegisterRuntimeNodeCoefficients("TestProvider", map[NodeCoefficient]float64{
		{Region: "r3", InstanceType: "c"}: 3.0,
	})
	if _, ok := runtimeNodeCoefficient("TestProvider", "r1", "a"); ok {
		t.Fatal("stale entry survived whole-set replacement")
	}
	if c, ok := runtimeNodeCoefficient("TestProvider", "r3", "c"); !ok || c != 3.0 {
		t.Fatalf("new set entry = (%v, %v), want (3.0, true)", c, ok)
	}

	// Clearing removes the provider.
	RegisterRuntimeNodeCoefficients("TestProvider", nil)
	if _, ok := runtimeNodeCoefficient("TestProvider", "r3", "c"); ok {
		t.Fatal("cleared provider still resolves")
	}
}

// TestRegistry_ConcurrentRegisterLookup hammers the registry from many
// goroutines; run with -race to prove thread-safety.
func TestRegistry_ConcurrentRegisterLookup(t *testing.T) {
	const workers = 16
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 250; j++ {
				RegisterRuntimeNodeCoefficients("RaceProvider", map[NodeCoefficient]float64{
					{Region: fmt.Sprintf("r%d", j%4), InstanceType: "a"}: float64(j),
				})
				runtimeNodeCoefficient("RaceProvider", "r0", "a")
			}
		}(i)
	}
	wg.Wait()
	RegisterRuntimeNodeCoefficients("RaceProvider", nil)
}
