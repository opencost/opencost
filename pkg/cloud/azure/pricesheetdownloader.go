package azure

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/profiles/2020-09-01/commerce/mgmt/commerce"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/pkg/cloud/httputil"
)

type PriceSheetDownloader struct {
	TenantID         string
	ClientID         string
	ClientSecret     string
	BillingAccount   string
	OfferID          string
	ConvertMeterInfo func(info commerce.MeterInfo) (map[string]*AzurePricing, error)
}

func (d *PriceSheetDownloader) GetPricing(ctx context.Context) (map[string]*AzurePricing, error) {
	log.Infof("requesting pricesheet download link")
	url, err := d.getDownloadURL(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting download URL: %w", err)
	}
	log.Infof("downloading pricesheet from %q", url)
	data, err := d.saveData(ctx, url, "pricesheet")
	if err != nil {
		return nil, fmt.Errorf("saving pricesheet from %q: %w", url, err)
	}
	defer data.Close()

	prices, err := d.readPriceSheet(ctx, data.File)
	if err != nil {
		return nil, fmt.Errorf("reading pricesheet: %w", err)
	}
	log.Infof("loaded %d pricings from pricesheet", len(prices))
	return prices, nil
}

func (d *PriceSheetDownloader) getDownloadURL(ctx context.Context) (string, error) {
	cred, err := azidentity.NewClientSecretCredential(d.TenantID, d.ClientID, d.ClientSecret, nil)
	if err != nil {
		return "", fmt.Errorf("creating credential: %w", err)
	}
	return priceSheetDownloadURL(ctx, cred, nil, d.BillingAccount, currentBillingPeriod())
}

func (d PriceSheetDownloader) saveData(ctx context.Context, url, tempName string) (*removeOnClose, error) {
	// Download file from URL in response.
	out, err := os.CreateTemp("", tempName)
	if err != nil {
		return nil, fmt.Errorf("creating %s temp file: %w", tempName, err)
	}

	// The price sheet can be large, so the streaming client bounds connect/TLS/
	// response-header time but not the body read, avoiding truncation of a slow
	// download. Pass the caller's context so the download is cancelable.
	resp, err := httputil.StreamingGet(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("downloading: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected HTTP status %d", resp.StatusCode)
	}

	if _, err := io.Copy(out, resp.Body); err != nil {
		return nil, fmt.Errorf("reading response: %w", err)
	}

	_, err = out.Seek(0, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("seeking to start of file: %w", err)
	}

	return &removeOnClose{File: out}, nil
}

type removeOnClose struct {
	*os.File
}

func (r *removeOnClose) Close() error {
	err := r.File.Close()
	if err != nil {
		return err
	}
	return os.Remove(r.Name())
}

// zipMagic is the local file header signature at the start of every zip archive.
var zipMagic = []byte{'P', 'K', 0x03, 0x04}

// readPriceSheet parses a downloaded price sheet. Since January 2023 the EA
// price sheet is delivered as a zip archive containing one or more CSV parts
// (each capped at 75MB), but a bare CSV is still accepted so that other billing
// account types and older sheets keep working.
func (d *PriceSheetDownloader) readPriceSheet(ctx context.Context, file *os.File) (map[string]*AzurePricing, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat downloaded file: %w", err)
	}

	prefix := make([]byte, len(zipMagic))
	n, err := io.ReadFull(file, prefix)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("reading downloaded file: %w", err)
	}
	isZip := bytes.Equal(prefix[:n], zipMagic)

	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seeking to start of file: %w", err)
	}

	if isZip {
		return d.readPricesheetZip(ctx, file, info.Size())
	}
	return d.readPricesheet(ctx, file)
}

// readPricesheetZip parses every CSV member of a zipped price sheet and merges
// the results. Individual members are allowed to fail as long as at least one
// parses, so a single malformed part can't discard the whole sheet.
func (d *PriceSheetDownloader) readPricesheetZip(ctx context.Context, data io.ReaderAt, size int64) (map[string]*AzurePricing, error) {
	archive, err := zip.NewReader(data, size)
	if err != nil {
		return nil, fmt.Errorf("opening zip: %w", err)
	}

	results := make(map[string]*AzurePricing)
	var (
		totals   pricesheetStats
		parsed   int
		csvFiles int
		errs     []error
	)
	for _, entry := range archive.File {
		if entry.FileInfo().IsDir() {
			continue
		}
		if !strings.EqualFold(path.Ext(entry.Name), ".csv") {
			log.Debugf("skipping non-CSV pricesheet entry %q", entry.Name)
			continue
		}
		csvFiles++
		entryResults, stats, err := d.readPricesheetZipEntry(ctx, entry)
		if err != nil {
			log.Warnf("reading pricesheet entry %q: %v", entry.Name, err)
			errs = append(errs, fmt.Errorf("%s: %w", entry.Name, err))
			continue
		}
		parsed++
		totals.add(stats)
		for key, pricing := range entryResults {
			results[key] = pricing
		}
	}

	if csvFiles == 0 {
		return nil, errors.New("pricesheet zip contained no CSV files")
	}
	if parsed == 0 {
		return nil, fmt.Errorf("no readable CSV in pricesheet zip: %w", errors.Join(errs...))
	}
	if len(results) == 0 {
		return nil, d.noMatchingPricingError(totals)
	}
	logPricesheetUnits(totals.units)
	return results, nil
}

func (d *PriceSheetDownloader) readPricesheetZipEntry(ctx context.Context, entry *zip.File) (map[string]*AzurePricing, pricesheetStats, error) {
	contents, err := entry.Open()
	if err != nil {
		return nil, pricesheetStats{}, fmt.Errorf("opening: %w", err)
	}
	defer contents.Close()
	return d.parsePricesheet(ctx, contents)
}

// readPricesheet parses a single price sheet CSV.
func (d *PriceSheetDownloader) readPricesheet(ctx context.Context, data io.Reader) (map[string]*AzurePricing, error) {
	results, stats, err := d.parsePricesheet(ctx, data)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, d.noMatchingPricingError(stats)
	}
	logPricesheetUnits(stats.units)
	return results, nil
}

// pricesheetStats records what a pass over a CSV skipped, so that a sheet which
// yields no usable prices can explain itself.
type pricesheetStats struct {
	skippedPriceType int
	skippedOffer     int
	// hasOfferColumn is true if any parsed CSV carried an offer ID column.
	hasOfferColumn bool
	// units holds every unit seen for a price we cared about.
	units map[string]bool
}

func (s *pricesheetStats) add(other pricesheetStats) {
	s.skippedPriceType += other.skippedPriceType
	s.skippedOffer += other.skippedOffer
	s.hasOfferColumn = s.hasOfferColumn || other.hasOfferColumn
	for unit := range other.units {
		if s.units == nil {
			s.units = make(map[string]bool)
		}
		s.units[unit] = true
	}
}

// parsePricesheet reads a price sheet CSV without judging whether the result is
// usable, so that zipped sheets can be assessed across all their parts.
func (d *PriceSheetDownloader) parsePricesheet(ctx context.Context, data io.Reader) (map[string]*AzurePricing, pricesheetStats, error) {
	// Avoid double-buffering.
	buf, ok := (data).(*bufio.Reader)
	if !ok {
		buf = bufio.NewReader(data)
	}

	cols, header, err := readPricesheetHeader(buf)
	if err != nil {
		return nil, pricesheetStats{}, err
	}

	reader := csv.NewReader(buf)
	reader.ReuseRecord = true
	// We consumed the header ourselves, so tell the reader how wide records are
	// in order to keep its row length checks.
	reader.FieldsPerRecord = len(header)

	stats := pricesheetStats{
		hasOfferColumn: cols.offerID >= 0,
		units:          make(map[string]bool),
	}
	results := make(map[string]*AzurePricing)
	lines := 1
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		lines++
		if err != nil {
			return nil, stats, fmt.Errorf("reading line %d: %w", lines, err)
		}

		// Only consumption prices are useful here. Savings plan and reserved
		// instance rows price a commitment rather than an hour of usage - the
		// reserved instance unit price is the whole one or three year total -
		// and we don't know whether the user has bought either, so including
		// them would let them overwrite the on-demand price for the same meter.
		if !isConsumptionPriceType(row[cols.priceType]) {
			stats.skippedPriceType++
			continue
		}

		// The offer ID column disappeared from the current EA schema. Where it
		// is still present a single meter can appear once per offer, so we have
		// to pick out the one the customer is actually billed under.
		if cols.offerID >= 0 && row[cols.offerID] != d.OfferID {
			stats.skippedOffer++
			continue
		}

		// TODO: Creating a meter info for each record will cause a
		// lot of GC churn - is it worth reusing one meter info instead?
		meterInfo, err := makeMeterInfo(cols, row)
		if err != nil {
			log.Warnf("making meter info (line %d): %v", lines, err)
			continue
		}

		pricings, err := d.ConvertMeterInfo(meterInfo)
		if err != nil {
			log.Warnf("converting meter to pricings (line %d): %v", lines, err)
			continue
		}

		if pricings != nil {
			stats.units[*meterInfo.Unit] = true
		}

		for key, pricing := range pricings {
			results[key] = pricing
		}
	}

	return results, stats, nil
}

// logPricesheetUnits records the units seen so we can detect any that still need
// handling in the conversions table.
func logPricesheetUnits(units map[string]bool) {
	allUnits := make([]string, 0, len(units))
	for unit := range units {
		allUnits = append(allUnits, unit)
	}
	sort.Strings(allUnits)
	log.Infof("all units in pricesheet: %s", strings.Join(allUnits, ", "))
}

// noMatchingPricingError explains why nothing was extracted. A mismatched offer
// ID is the most common cause and it used to be invisible.
func (d *PriceSheetDownloader) noMatchingPricingError(stats pricesheetStats) error {
	const msg = "no matching pricing from price sheet"
	var details []string
	if stats.skippedPriceType > 0 {
		details = append(details, fmt.Sprintf("%d rows skipped as non-consumption prices", stats.skippedPriceType))
	}
	if stats.skippedOffer > 0 {
		details = append(details, fmt.Sprintf("%d rows skipped because their offer ID did not match %q (set AZURE_OFFER_ID to the offer for your enrollment)", stats.skippedOffer, d.OfferID))
	}
	if !stats.hasOfferColumn {
		details = append(details, "price sheet has no offer ID column, so no offer filtering was applied")
	}
	if len(details) == 0 {
		return errors.New(msg)
	}
	return fmt.Errorf("%s: %s", msg, strings.Join(details, "; "))
}

// isConsumptionPriceType reports whether a "Price type" / "priceType" cell
// denotes a pay-as-you-go price. Blank counts as consumption because the legacy
// sheet left the column empty for some meters.
func isConsumptionPriceType(priceType string) bool {
	switch normaliseColumnName(priceType) {
	case "", "consumption":
		return true
	default:
		return false
	}
}

// maxPreambleLines is how far we'll look for the header row. The legacy sheet
// began with a title line and a blank line; the current one starts with the
// header.
const maxPreambleLines = 4

// readPricesheetHeader consumes everything up to and including the header row,
// leaving buf positioned at the first data row.
func readPricesheetHeader(buf *bufio.Reader) (priceSheetColumns, []string, error) {
	var headerErr error
	for i := 0; i < maxPreambleLines; i++ {
		line, err := buf.ReadString('\n')
		if line == "" {
			if err != nil {
				break
			}
			continue
		}

		line = strings.TrimRight(line, "\r\n")
		if strings.TrimSpace(line) == "" {
			continue
		}

		fields, parseErr := parseCSVLine(line)
		if parseErr != nil {
			headerErr = fmt.Errorf("reading header: %w", parseErr)
			continue
		}

		cols, colsErr := newPriceSheetColumns(fields)
		if colsErr == nil {
			return cols, fields, nil
		}
		headerErr = colsErr

		if err != nil {
			break
		}
	}
	if headerErr == nil {
		headerErr = errors.New("no header row found in price sheet")
	}
	return priceSheetColumns{}, nil, headerErr
}

func parseCSVLine(line string) ([]string, error) {
	reader := csv.NewReader(strings.NewReader(line))
	reader.FieldsPerRecord = -1
	return reader.Read()
}

// priceSheetColumns records where the fields we need live in a price sheet row.
// Columns are looked up by name rather than position because the Cost Management
// schema renamed and reordered them relative to the Consumption one.
type priceSheetColumns struct {
	meterName        int
	meterCategory    int
	meterSubCategory int
	meterRegion      int
	// unit is "Unit" on the legacy sheet and "UnitOfMeasure" on the current one.
	unit      int
	unitPrice int
	priceType int
	// offerID is -1 when the sheet has no offer ID column.
	offerID int
}

func newPriceSheetColumns(header []string) (priceSheetColumns, error) {
	index := make(map[string]int, len(header))
	for i, name := range header {
		normalised := normaliseColumnName(name)
		if normalised == "" {
			continue
		}
		if _, seen := index[normalised]; !seen {
			index[normalised] = i
		}
	}

	cols := priceSheetColumns{offerID: -1}
	// Each entry lists the accepted spellings for a column, most preferred
	// first, so one table covers both schemas.
	required := []struct {
		name    string
		target  *int
		aliases []string
	}{
		{"MeterName", &cols.meterName, []string{"metername"}},
		{"MeterCategory", &cols.meterCategory, []string{"metercategory"}},
		{"MeterSubCategory", &cols.meterSubCategory, []string{"metersubcategory"}},
		{"MeterRegion", &cols.meterRegion, []string{"meterregion"}},
		{"UnitOfMeasure", &cols.unit, []string{"unit", "unitofmeasure"}},
		{"UnitPrice", &cols.unitPrice, []string{"unitprice"}},
		{"PriceType", &cols.priceType, []string{"pricetype"}},
	}
	for _, column := range required {
		found := false
		for _, alias := range column.aliases {
			if i, ok := index[alias]; ok {
				*column.target = i
				found = true
				break
			}
		}
		if !found {
			return priceSheetColumns{}, fmt.Errorf("price sheet header is missing a %q column, got %q", column.name, strings.Join(header, ","))
		}
	}

	if i, ok := index["offerid"]; ok {
		cols.offerID = i
	}

	return cols, nil
}

// normaliseColumnName reduces a header cell to a comparable form, so that
// "Meter sub-category" and "MeterSubCategory" both match.
func normaliseColumnName(name string) string {
	name = strings.TrimPrefix(name, "\ufeff")
	name = strings.Map(func(r rune) rune {
		switch r {
		case ' ', '\t', '-', '_':
			return -1
		}
		return r
	}, name)
	return strings.ToLower(name)
}

func makeMeterInfo(cols priceSheetColumns, row []string) (commerce.MeterInfo, error) {
	price, err := strconv.ParseFloat(strings.TrimSpace(row[cols.unitPrice]), 64)
	if err != nil {
		return commerce.MeterInfo{}, fmt.Errorf("parsing unit price: %w", err)
	}
	newPrice, unit := normalisePrice(price, row[cols.unit])
	return commerce.MeterInfo{
		MeterName:        ptr(row[cols.meterName]),
		MeterCategory:    ptr(row[cols.meterCategory]),
		MeterSubCategory: ptr(row[cols.meterSubCategory]),
		Unit:             &unit,
		MeterRegion:      ptr(row[cols.meterRegion]),
		MeterRates:       map[string]*float64{"0": &newPrice},
	}, nil
}

func currentBillingPeriod() string {
	return time.Now().Format("200601")
}

func ptr[T any](v T) *T {
	return &v
}

// conversions lists all the units seen from the price sheet for
// prices we're interested in with factors to the corresponding units
// in the rate card.
var conversions = map[string]struct {
	divisor float64
	unit    string
}{
	"1 /Month":       {divisor: 1, unit: "1 /Month"},
	"1 Hour":         {divisor: 1, unit: "1 Hour"},
	"1 PiB/Hour":     {divisor: 1_000_000, unit: "1 GiB/Hour"},
	"10 /Month":      {divisor: 10, unit: "1 /Month"},
	"10 Hours":       {divisor: 10, unit: "1 Hour"},
	"100 /Month":     {divisor: 100, unit: "1 /Month"},
	"100 GB/Month":   {divisor: 100, unit: "1 GB/Month"},
	"100 Hours":      {divisor: 100, unit: "1 Hour"},
	"100 TiB/Hour":   {divisor: 100_000, unit: "1 GiB/Hour"},
	"1000 Hours":     {divisor: 1000, unit: "1 Hour"},
	"10000 Hours":    {divisor: 10_000, unit: "1 Hour"},
	"100000 /Hour":   {divisor: 100_000, unit: "1 /Hour"},
	"1000000 /Hour":  {divisor: 1_000_000, unit: "1 /Hour"},
	"10000000 /Hour": {divisor: 10_000_000, unit: "1 /Hour"},
}

func normalisePrice(price float64, unit string) (float64, string) {
	if conv, ok := conversions[unit]; ok {
		return price / conv.divisor, conv.unit
	}

	return price, unit
}
