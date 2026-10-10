package carbon

import "sync"

// NodeCoefficient identifies a node carbon coefficient registered at runtime:
// the provider's nodes of the given instance type in the given region.
type NodeCoefficient struct {
	Region       string
	InstanceType string
}

// runtimeNodeCoefficients holds per-provider node carbon coefficients
// (tonnes CO2e per hour) registered by cloud providers at runtime — e.g.
// Scaleway registers them from its public product catalog. The registry is
// consulted before the static embedded lookup table, and only for providers
// that have no rows in the static table (see lookupCarbonCoeff).
var (
	runtimeCoefficientsMu sync.RWMutex
	runtimeNodeCoeffs     map[string]map[NodeCoefficient]float64
)

// RegisterRuntimeNodeCoefficients atomically replaces the runtime node
// coefficient set for the given provider. Passing nil clears the set.
// Coefficients are in tonnes CO2e per hour, the same unit as the static
// embedded lookup table.
func RegisterRuntimeNodeCoefficients(provider string, coeffs map[NodeCoefficient]float64) {
	runtimeCoefficientsMu.Lock()
	defer runtimeCoefficientsMu.Unlock()

	if runtimeNodeCoeffs == nil {
		runtimeNodeCoeffs = make(map[string]map[NodeCoefficient]float64)
	}
	if coeffs == nil {
		delete(runtimeNodeCoeffs, provider)
		return
	}
	runtimeNodeCoeffs[provider] = coeffs
}

// runtimeNodeCoefficient returns the runtime-registered coefficient for the
// given provider, region, and instance type.
func runtimeNodeCoefficient(provider, region, instanceType string) (float64, bool) {
	runtimeCoefficientsMu.RLock()
	defer runtimeCoefficientsMu.RUnlock()

	coeffs, ok := runtimeNodeCoeffs[provider]
	if !ok {
		return 0, false
	}
	coeff, ok := coeffs[NodeCoefficient{Region: region, InstanceType: instanceType}]
	return coeff, ok
}
