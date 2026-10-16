package scaleway

import (
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/pkg/carbon"
)

// catalogCarbonCoefficients converts the store's carbon section (kg CO2e per
// product unit, hourly units only) to the registry's unit (tonnes CO2e per
// hour), keyed by (region, offerID).
func catalogCarbonCoefficients(store *catalogStore) map[carbon.NodeCoefficient]float64 {
	coeffs := make(map[carbon.NodeCoefficient]float64, len(store.carbon))
	for key, kgCO2ePerHour := range store.carbon {
		coeffs[carbon.NodeCoefficient{Region: key.Region, InstanceType: key.OfferID}] = kgCO2ePerHour / 1000.0
	}
	return coeffs
}

// registerCatalogCarbonCoefficients publishes the catalog-derived node carbon
// coefficients to the carbon package after a successful catalog refresh.
//
// The catalog expresses impact in kg CO2e per product unit; the carbon lookup
// table expects tonnes CO2e per hour, so the value is divided by 1000 (R11).
// It is a no-op when no in-scope instance carries impact data (FR-013), which
// leaves the previous coefficient set intact (data-model.md §5).
func registerCatalogCarbonCoefficients(store *catalogStore) {
	if store == nil {
		return
	}
	coeffs := catalogCarbonCoefficients(store)
	if len(coeffs) == 0 {
		return
	}
	carbon.RegisterRuntimeNodeCoefficients(opencost.ScalewayProvider, coeffs)
}
