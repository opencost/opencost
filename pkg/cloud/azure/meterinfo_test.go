package azure

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/commerce/armcommerce"
	"github.com/stretchr/testify/require"
)

func TestWidenRate(t *testing.T) {
	for _, price := range []float64{0.096, 0.000041, 135.17, 5.2795, 0.3, 1234.567} {
		require.Equal(t, price, widenRate(float32(price)))
		require.NotEqual(t, price, float64(float32(price)), "test value %v must not survive a plain float32 conversion", price)
	}
	require.Equal(t, float64(0), widenRate(0))
}

func TestMeterInfoFromRateCard(t *testing.T) {
	info := meterInfoFromRateCard(&armcommerce.MeterInfo{
		MeterName:        to.Ptr("D2s v3"),
		MeterCategory:    to.Ptr("Virtual Machines"),
		MeterSubCategory: to.Ptr("Dsv3 Series"),
		MeterRegion:      to.Ptr("EU North"),
		Unit:             to.Ptr("1 Hour"),
		MeterRates: map[string]*float32{
			"0":   to.Ptr[float32](0.096),
			"100": nil,
		},
	})
	require.Equal(t, meterInfo{
		MeterName:        "D2s v3",
		MeterCategory:    "Virtual Machines",
		MeterSubCategory: "Dsv3 Series",
		MeterRegion:      "EU North",
		Unit:             "1 Hour",
		MeterRates:       map[string]float64{"0": 0.096},
	}, info)

	empty := meterInfoFromRateCard(&armcommerce.MeterInfo{})
	require.Equal(t, meterInfo{MeterRates: map[string]float64{}}, empty)
}
