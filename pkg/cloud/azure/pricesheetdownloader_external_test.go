package azure_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/opencost/opencost/pkg/cloud/azure"
)

// Callers outside the package must be able to supply their own converter.
func TestPriceSheetDownloaderExternalConverter(t *testing.T) {
	var got azure.MeterInfo
	d := azure.PriceSheetDownloader{
		ConvertMeterInfo: func(info azure.MeterInfo) (map[string]*azure.AzurePricing, error) {
			got = info
			return nil, nil
		},
	}
	_, err := d.ConvertMeterInfo(azure.MeterInfo{MeterName: "D2s v3"})
	require.NoError(t, err)
	require.Equal(t, "D2s v3", got.MeterName)
}
