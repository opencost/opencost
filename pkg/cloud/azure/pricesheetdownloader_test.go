package azure

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/profiles/2020-09-01/commerce/mgmt/commerce"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testDownloader() PriceSheetDownloader {
	return PriceSheetDownloader{
		TenantID:         "test-tenant-id",
		ClientID:         "test-client-id",
		ClientSecret:     "test-client-secret",
		BillingAccount:   "test-billing-account",
		OfferID:          "my-offer-id",
		ConvertMeterInfo: convertMeter,
	}
}

// expectedPrices is what both the legacy and the current price sheet schema
// should boil down to, since the fixtures describe the same meters.
var expectedPrices = map[string]*AzurePricing{
	"DC96as_v4 1 Hour": {Node: &models.Node{Cost: "10.505"}},
	"DC2as_v4 1 Hour":  {Node: &models.Node{Cost: "0.219"}},
	"VM1 1 Hour":       {Node: &models.Node{Cost: "1.0"}},
	"VM2 1 Hour":       {Node: &models.Node{Cost: "2.0"}},
}

func TestDownloader(t *testing.T) {
	d := testDownloader()

	// The legacy Microsoft.Consumption sheet: a title line, a blank line, then
	// 14 fixed-position columns. Still parsed so nothing regresses for anyone
	// reading an archived sheet.
	t.Run("read prices from legacy schema", func(t *testing.T) {
		results, err := d.readPricesheet(context.Background(), strings.NewReader(legacyPricesheetData))
		require.NoError(t, err)

		// Units and prices are normalised.
		// Info for saving plans and other offers is skipped.
		require.Equal(t, expectedPrices, results)
	})

	// The current Microsoft.CostManagement sheet: no preamble, 21 renamed and
	// reordered columns, no offer ID, and reserved instance rows included.
	t.Run("read prices from current schema", func(t *testing.T) {
		results, err := d.readPricesheet(context.Background(), strings.NewReader(currentPricesheetData))
		require.NoError(t, err)
		require.Equal(t, expectedPrices, results)
	})

	// A reserved instance unit price is the total cost of a one or three year
	// commitment, so letting one through would wildly inflate an hourly rate.
	t.Run("reserved instance rows do not overwrite consumption prices", func(t *testing.T) {
		results, err := d.readPricesheet(context.Background(), strings.NewReader(currentPricesheetData))
		require.NoError(t, err)

		require.Contains(t, results, "DC96as_v4 1 Hour")
		assert.Equal(t, "10.505", results["DC96as_v4 1 Hour"].Node.Cost,
			"consumption price must win over the ReservedInstance row for the same meter")
	})

	t.Run("savings plan rows are skipped in both spellings", func(t *testing.T) {
		for _, priceType := range []string{"Savings Plan", "SavingsPlan"} {
			data := currentHeader + "\n" + currentRow("DC16as_v4", priceType, "10 Hours", "17.51") + "\n"
			_, err := d.readPricesheet(context.Background(), strings.NewReader(data))
			require.ErrorContains(t, err, "no matching pricing from price sheet", "price type %q should be skipped", priceType)
		}
	})

	t.Run("handles CRLF line endings and a UTF-8 BOM", func(t *testing.T) {
		data := "\ufeff" + strings.ReplaceAll(currentPricesheetData, "\n", "\r\n")
		results, err := d.readPricesheet(context.Background(), strings.NewReader(data))
		require.NoError(t, err)
		require.Equal(t, expectedPrices, results)
	})

	t.Run("bad header", func(t *testing.T) {
		data := "\n\nMeter ID,Meter name,Meter category,Something else,,,,,,,,,,,,,,\n"
		_, err := d.readPricesheet(context.Background(), strings.NewReader(data))
		require.ErrorContains(t, err, `price sheet header is missing a "MeterSubCategory" column`)
	})

	t.Run("short header", func(t *testing.T) {
		data := "\n\nMeter ID, Meter name, Meter category, Meter sub-category\n"
		_, err := d.readPricesheet(context.Background(), strings.NewReader(data))
		require.ErrorContains(t, err, `price sheet header is missing a "MeterRegion" column`)
	})

	t.Run("no header at all", func(t *testing.T) {
		_, err := d.readPricesheet(context.Background(), strings.NewReader(""))
		require.ErrorContains(t, err, "no header row found in price sheet")
	})

	t.Run("no matching prices", func(t *testing.T) {
		d := testDownloader()
		d.ConvertMeterInfo = func(commerce.MeterInfo) (map[string]*AzurePricing, error) {
			return nil, nil
		}
		_, err := d.readPricesheet(context.Background(), strings.NewReader(legacyPricesheetData))
		require.ErrorContains(t, err, "no matching pricing from price sheet")
	})

	// A mismatched AZURE_OFFER_ID silently produced an empty sheet before, which
	// was near impossible to diagnose from the logs.
	t.Run("offer ID mismatch is explained", func(t *testing.T) {
		d := testDownloader()
		d.OfferID = "not-the-offer-in-the-sheet"
		_, err := d.readPricesheet(context.Background(), strings.NewReader(legacyPricesheetData))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "offer ID did not match")
		assert.Contains(t, err.Error(), "not-the-offer-in-the-sheet")
		assert.Contains(t, err.Error(), "AZURE_OFFER_ID")
	})

	// The current schema has no offer ID column, so say so rather than leaving
	// people hunting for an offer configuration problem that doesn't exist.
	t.Run("missing offer ID column is explained", func(t *testing.T) {
		d := testDownloader()
		d.ConvertMeterInfo = func(commerce.MeterInfo) (map[string]*AzurePricing, error) {
			return nil, nil
		}
		_, err := d.readPricesheet(context.Background(), strings.NewReader(currentPricesheetData))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no offer ID column")
	})
}

// Since January 2023 the sheet arrives as a zip of CSV parts, each capped at
// 75MB, rather than as a single CSV.
func TestReadPriceSheetZip(t *testing.T) {
	d := testDownloader()

	t.Run("merges every CSV part", func(t *testing.T) {
		archive := buildZip(t, map[string]string{
			"pricesheet_part_1.csv": currentHeader + "\n" +
				currentRow("DC96as_v4", "Consumption", "10 Hours", "105.05") + "\n",
			"pricesheet_part_2.csv": currentHeader + "\n" +
				currentRow("DC2as_v4", "Consumption", "100 Hours", "21.90") + "\n" +
				currentRow("multiple-prices", "Consumption", "10 Hours", "105.05") + "\n",
			// Non-CSV members must be ignored rather than fail the parse.
			"manifest.json": `{"partCount":2}`,
		})

		results, err := d.readPriceSheet(context.Background(), tempFileWith(t, archive))
		require.NoError(t, err)
		require.Equal(t, expectedPrices, results)
	})

	// One unreadable part shouldn't throw away a whole sheet's pricing.
	t.Run("tolerates one unreadable part", func(t *testing.T) {
		archive := buildZip(t, map[string]string{
			"part_1.csv": "totally,unrelated,columns\n1,2,3\n",
			"part_2.csv": currentHeader + "\n" +
				currentRow("DC96as_v4", "Consumption", "10 Hours", "105.05") + "\n",
		})

		results, err := d.readPriceSheet(context.Background(), tempFileWith(t, archive))
		require.NoError(t, err)
		assert.Equal(t, map[string]*AzurePricing{
			"DC96as_v4 1 Hour": {Node: &models.Node{Cost: "10.505"}},
		}, results)
	})

	t.Run("fails when no part is readable", func(t *testing.T) {
		archive := buildZip(t, map[string]string{
			"part_1.csv": "totally,unrelated,columns\n1,2,3\n",
		})

		_, err := d.readPriceSheet(context.Background(), tempFileWith(t, archive))
		require.ErrorContains(t, err, "no readable CSV in pricesheet zip")
	})

	t.Run("fails when the zip has no CSV", func(t *testing.T) {
		archive := buildZip(t, map[string]string{"manifest.json": `{}`})

		_, err := d.readPriceSheet(context.Background(), tempFileWith(t, archive))
		require.ErrorContains(t, err, "pricesheet zip contained no CSV files")
	})

	t.Run("reports when parts parse but match nothing", func(t *testing.T) {
		archive := buildZip(t, map[string]string{
			"part_1.csv": currentHeader + "\n" +
				currentRow("DC16as_v4", "ReservedInstance", "10 Hours", "90000") + "\n",
		})

		_, err := d.readPriceSheet(context.Background(), tempFileWith(t, archive))
		require.ErrorContains(t, err, "no matching pricing from price sheet")
		assert.Contains(t, err.Error(), "non-consumption prices")
	})
}

// A bare CSV must still work, both for archived sheets and for any scope that
// isn't handed a zip.
func TestReadPriceSheetPlainCSV(t *testing.T) {
	d := testDownloader()

	results, err := d.readPriceSheet(context.Background(), tempFileWith(t, []byte(currentPricesheetData)))
	require.NoError(t, err)
	require.Equal(t, expectedPrices, results)
}

func TestReadPriceSheetEmptyFile(t *testing.T) {
	d := testDownloader()

	_, err := d.readPriceSheet(context.Background(), tempFileWith(t, nil))
	require.ErrorContains(t, err, "no header row found in price sheet")
}

// Diagnostics should quote the real line in the file, so a preamble must not
// shift the numbering.
func TestReadPricesheetHeaderReportsLinesConsumed(t *testing.T) {
	tests := []struct {
		name     string
		data     string
		expected int
	}{
		{
			name:     "current schema starts at the header",
			data:     currentHeader + "\nrow\n",
			expected: 1,
		},
		{
			name:     "legacy schema has a title and a blank line first",
			data:     legacyPricesheetData,
			expected: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, consumed, err := readPricesheetHeader(bufio.NewReader(strings.NewReader(tt.data)))
			require.NoError(t, err)
			assert.Equal(t, tt.expected, consumed)
		})
	}
}

// A bad row in a legacy sheet sits at line 4 (title, blank, header, then data),
// so that is what the warning has to say.
func TestParsePricesheetLineNumbersMatchTheFile(t *testing.T) {
	d := testDownloader()

	// An unparseable unit price on the first data row.
	data := legacyHeader + "\nid,VM,Virtual Machines,Series,US East,1 Hour,1 Hour,PN,not-a-number,USD,0,my-offer-id,,Consumption\n"
	_, _, err := d.parsePricesheet(context.Background(), "", strings.NewReader(data))
	require.NoError(t, err, "a bad price is skipped, not fatal")

	// A malformed row (too few fields) is fatal, and reports its line.
	broken := legacyHeader + "\nonly,three,fields\n"
	_, _, err = d.parsePricesheet(context.Background(), "", strings.NewReader(broken))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "line 2", "header is line 1, so the first data row is line 2")

	// The same row inside a zip part names the part it came from.
	_, _, err = d.parsePricesheet(context.Background(), "part_2.csv", strings.NewReader(broken))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "part_2.csv line 2")

	// With a legacy preamble the same row is physically on line 4: title,
	// blank, header, data. Counting data rows instead would report 1 here.
	withPreamble := "Price Sheet Report for billing period - 202304\n\n" + broken
	_, _, err = d.parsePricesheet(context.Background(), "", strings.NewReader(withPreamble))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "line 4", "the preamble must count towards the line number")
}

func TestNewPriceSheetColumns(t *testing.T) {
	t.Run("legacy schema", func(t *testing.T) {
		cols, err := newPriceSheetColumns(strings.Split(legacyHeader, ","))
		require.NoError(t, err)
		assert.Equal(t, 1, cols.meterName)
		assert.Equal(t, 5, cols.unit, "legacy sheets have a Unit column distinct from Unit of measure")
		assert.Equal(t, 8, cols.unitPrice)
		assert.Equal(t, 11, cols.offerID)
		assert.Equal(t, 13, cols.priceType)
	})

	t.Run("current schema", func(t *testing.T) {
		cols, err := newPriceSheetColumns(strings.Split(currentHeader, ","))
		require.NoError(t, err)
		assert.Equal(t, 8, cols.meterName)
		assert.Equal(t, 19, cols.unit, "current sheets only have UnitOfMeasure")
		assert.Equal(t, 20, cols.unitPrice)
		assert.Equal(t, 13, cols.priceType)
		assert.Equal(t, -1, cols.offerID, "current schema has no offer ID column")
	})

	// Positions are resolved by name, so reordering must not matter.
	t.Run("column order is irrelevant", func(t *testing.T) {
		cols, err := newPriceSheetColumns([]string{
			"UnitPrice", "priceType", "MeterRegion", "MeterSubCategory",
			"MeterCategory", "MeterName", "UnitOfMeasure",
		})
		require.NoError(t, err)
		assert.Equal(t, 0, cols.unitPrice)
		assert.Equal(t, 5, cols.meterName)
		assert.Equal(t, 6, cols.unit)
	})
}

func TestNormaliseColumnName(t *testing.T) {
	// Both schemas' spellings of each column must collapse to the same key.
	pairs := [][2]string{
		{"Meter sub-category", "MeterSubCategory"},
		{"Meter name", "MeterName"},
		{"Unit of measure", "UnitOfMeasure"},
		{"Price type", "priceType"},
		{"Offer Id", "OfferID"},
		{"\ufeffMeter ID", "MeterId"},
	}
	for _, pair := range pairs {
		assert.Equal(t, normaliseColumnName(pair[0]), normaliseColumnName(pair[1]),
			"%q and %q should normalise the same", pair[0], pair[1])
	}
}

func TestIsConsumptionPriceType(t *testing.T) {
	for _, priceType := range []string{"Consumption", "consumption", "", " "} {
		assert.True(t, isConsumptionPriceType(priceType), "%q should count as consumption", priceType)
	}
	for _, priceType := range []string{"Savings Plan", "SavingsPlan", "ReservedInstance", "Reserved Instance"} {
		assert.False(t, isConsumptionPriceType(priceType), "%q should not count as consumption", priceType)
	}
}

func buildZip(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	// Sort so the archive's entry order is deterministic across runs.
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		f, err := w.Create(name)
		require.NoError(t, err)
		_, err = f.Write([]byte(files[name]))
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())
	return buf.Bytes()
}

func tempFileWith(t *testing.T, contents []byte) *os.File {
	t.Helper()
	path := filepath.Join(t.TempDir(), "pricesheet")
	require.NoError(t, os.WriteFile(path, contents, 0o600))
	f, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })
	return f
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

const legacyHeader = "Meter ID,Meter name,Meter category,Meter sub-category,Meter region,Unit,Unit of measure,Part number,Unit price,Currency code,Included quantity,Offer Id,Term,Price type"

// legacyPricesheetData is a sheet in the retired Microsoft.Consumption format.
const legacyPricesheetData = `Price Sheet Report for billing period - 202304

` + legacyHeader + `
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

// currentHeader is the EA price sheet schema returned by Cost Management, in the
// documented column order. Note there is no offer ID column, "Unit" is gone in
// favour of "UnitOfMeasure", and the price columns moved to the end.
const currentHeader = "BasePrice,CurrencyCode,EffectiveEndDate,EffectiveStartDate,IncludedQuantity,MarketPrice,MeterId,MeterCategory,MeterName,MeterSubCategory,MeterRegion,MeterType,PartNumber,priceType,Product,ProductID,ServiceFamily,SkuID,Term,UnitOfMeasure,UnitPrice"

// currentRow builds a row in the current schema for the given meter.
func currentRow(meterName, priceType, unitOfMeasure, unitPrice string) string {
	return strings.Join([]string{
		unitPrice,               // BasePrice
		"USD",                   // CurrencyCode
		"2026-08-31T00:00:00Z",  // EffectiveEndDate
		"2026-08-01T00:00:00Z",  // EffectiveStartDate
		"0",                     // IncludedQuantity
		unitPrice,               // MarketPrice
		"d4236f8f-3ba6-5a9a",    // MeterId
		"Virtual Machines",      // MeterCategory
		meterName,               // MeterName
		"DCasv4 Series",         // MeterSubCategory
		"US East",               // MeterRegion
		"ComputeHours",          // MeterType
		"AAF-70822",             // PartNumber
		priceType,               // priceType
		"Virtual Machines DCas", // Product
		"DZH318Z0BQ35",          // ProductID
		"Compute",               // ServiceFamily
		"DZH318Z0BQ35/00GN",     // SkuID
		"",                      // Term
		unitOfMeasure,           // UnitOfMeasure
		unitPrice,               // UnitPrice
	}, ",")
}

// currentPricesheetData describes the same meters as legacyPricesheetData in the
// current schema, plus the savings plan and reserved instance rows that the new
// sheet adds and that we must ignore.
var currentPricesheetData = strings.Join([]string{
	currentHeader,
	currentRow("DC96as_v4", "Consumption", "10 Hours", "105.05"),
	currentRow("DC2as_v4", "Consumption", "100 Hours", "21.90"),
	currentRow("DC16as_v4", "SavingsPlan", "10 Hours", "17.51"),
	// Deliberately after the consumption row for the same meter: if this were
	// not filtered out it would overwrite the hourly price with a three year
	// commitment total.
	currentRow("DC96as_v4", "ReservedInstance", "10 Hours", "90000.00"),
	currentRow("skip-this", "Consumption", "10 Hours", "105.05"),
	currentRow("multiple-prices", "Consumption", "10 Hours", "105.05"),
	currentRow("error", "Consumption", "10 Hours", "105.05"),
}, "\n") + "\n"
