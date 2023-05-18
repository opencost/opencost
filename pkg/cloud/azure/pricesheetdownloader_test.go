package azure

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/profiles/2020-09-01/commerce/mgmt/commerce"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/stretchr/testify/require"
)

func TestDownloader(t *testing.T) {
	d := PriceSheetDownloader{
		TenantID:         "test-tenant-id",
		ClientID:         "test-client-id",
		ClientSecret:     "test-client-secret",
		BillingAccount:   "test-billing-account",
		OfferID:          "my-offer-id",
		ConvertMeterInfo: convertMeter,
	}

	t.Run("read prices", func(t *testing.T) {
		results, err := d.readPricesheet(context.Background(), strings.NewReader(pricesheetData))
		require.NoError(t, err)

		// Units and prices are normalised.
		// Info for saving plans and other offers is skipped.
		expected := map[string]*AzurePricing{
			"DC96as_v4 1 Hour": {Node: &models.Node{Cost: "10.505"}},
			"DC2as_v4 1 Hour":  {Node: &models.Node{Cost: "0.219"}},
			"VM1 1 Hour":       {Node: &models.Node{Cost: "1.0"}},
			"VM2 1 Hour":       {Node: &models.Node{Cost: "2.0"}},
		}
		require.Equal(t, expected, results)
	})

	t.Run("bad header", func(t *testing.T) {
		data := "\n\nMeter ID,Meter name,Meter category,Something else,,,,,,,,,,,,,,\n"
		_, err := d.readPricesheet(context.Background(), strings.NewReader(data))
		require.ErrorContains(t, err, `unexpected header at col 3 "Something else", expected "Meter sub-category"`)
	})

	t.Run("short header", func(t *testing.T) {
		data := "\n\nMeter ID, Meter name, Meter category, Meter sub-category\n"
		_, err := d.readPricesheet(context.Background(), strings.NewReader(data))
		require.ErrorContains(t, err, "too few header columns: got 4, expected 14")
	})

	t.Run("no matching prices", func(t *testing.T) {
		d := PriceSheetDownloader{
			TenantID:       "test-tenant-id",
			ClientID:       "test-client-id",
			ClientSecret:   "test-client-secret",
			BillingAccount: "test-billing-account",
			OfferID:        "my-offer-id",
			ConvertMeterInfo: func(commerce.MeterInfo) (map[string]*AzurePricing, error) {
				return nil, nil
			},
		}
		_, err := d.readPricesheet(context.Background(), strings.NewReader(pricesheetData))
		require.ErrorContains(t, err, "no matching pricing from price sheet")
	})
}

func convertMeter(info commerce.MeterInfo) (map[string]*AzurePricing, error) {
	switch *info.MeterName {
	case "skip-this":
		return nil, nil
	case "multiple-prices":
		return map[string]*AzurePricing{
			"VM1 1 Hour": {Node: &models.Node{Cost: "1.0"}},
			"VM2 1 Hour": {Node: &models.Node{Cost: "2.0"}},
		}, nil
	case "error":
		return nil, fmt.Errorf("there was an error handling this row!")
	default:
		return map[string]*AzurePricing{
			*info.MeterName + " " + *info.Unit: {
				Node: &models.Node{Cost: fmt.Sprintf("%0.3f", *info.MeterRates["0"])},
			},
		}, nil
	}
}

const pricesheetData = `Price Sheet Report for billing period - 202304

Meter ID,Meter name,Meter category,Meter sub-category,Meter region,Unit,Unit of measure,Part number,Unit price,Currency code,Included quantity,Offer Id,Term,Price type
d4236f8f-3ba6-5a9a-8c6b-14556538c44c,DC96as_v4,Virtual Machines,DCasv4 Series,US East,10 Hours,10 Hours,AAF-70822,105.050000000000000,USD,0.00,my-offer-id,,Consumption
d4236f8f-3ba6-5a9a-8c6b-14556538c44c,DC96as_v4,Virtual Machines,DCasv4 Series,US East,10 Hours,10 Hours,AAF-70831,60.890000000000000,USD,0.00,other-offer-id,,Consumption
e47a2c4c-4dc4-55d5-a8d7-ec5b1dcc9c08,DC2as_v4,Virtual Machines,DCasv4 Series,US East,100 Hours,100 Hours,AAF-70890,21.900000000000000,USD,0.000,my-offer-id,,Consumption
e47a2c4c-4dc4-55d5-a8d7-ec5b1dcc9c08,DC2as_v4,Virtual Machines,DCasv4 Series,US East,100 Hours,100 Hours,AAF-70886,12.700000000000000,USD,0.000,other-offer-id,,Consumption
cb8d72c0-2b02-5b41-9ac9-2809c04f17ff,DC16as_v4,Virtual Machines,DCasv4 Series,US East,10 Hours,10 Hours,AAF-70911,17.510000000000000,USD,0.00,my-offer-id,,Savings Plan
cb8d72c0-2b02-5b41-9ac9-2809c04f17ff,DC16as_v4,Virtual Machines,DCasv4 Series,US East,10 Hours,10 Hours,AAF-70910,10.150000000000000,USD,0.00,other-offer-id,,Consumption
d4236f8f-3ba6-5a9a-8c6b-14556538c44c,skip-this,Virtual Machines,DCasv4 Series,US East,10 Hours,10 Hours,AAF-70822,105.050000000000000,USD,0.00,my-offer-id,,Consumption
d4236f8f-3ba6-5a9a-8c6b-14556538c44c,multiple-prices,Virtual Machines,DCasv4 Series,US East,10 Hours,10 Hours,AAF-70822,105.050000000000000,USD,0.00,my-offer-id,,Consumption
d4236f8f-3ba6-5a9a-8c6b-14556538c44c,error,Virtual Machines,DCasv4 Series,US East,10 Hours,10 Hours,AAF-70822,105.050000000000000,USD,0.00,my-offer-id,,Consumption
`
