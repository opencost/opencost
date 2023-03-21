package costmodel

import (
	"bytes"
	"context"
	"encoding/csv"
	"io"
	"sort"
	"strconv"
	"time"

	"github.com/pkg/errors"

	"github.com/opencost/opencost/pkg/kubecost"
	"github.com/opencost/opencost/pkg/log"
)

type CloudStorage interface {
	Write(name string, data []byte) error
	Read(name string) ([]byte, error)
	Exists(name string) (bool, error)
}

type AllocationModel interface {
	ComputeAllocation(start, end time.Time, resolution time.Duration) (*kubecost.AllocationSet, error)
	DateRange() (time.Time, time.Time, error)
}

var errNoData = errors.New("no data")

// UpdateCSVWorker launches a worker that updates CSV file in cloud storage with allocation data
// It updates data immediately on launch and then runs every day at 00:10 UTC
// It expected to run a goroutine
func UpdateCSVWorker(ctx context.Context, storage CloudStorage, model AllocationModel, path string) error {
	// perform first update immediately
	nextRunAt := time.Now()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(nextRunAt.Sub(time.Now())):
			err := UpdateCSV(ctx, storage, model, path)
			if err != nil {
				// it's background worker, log error and carry on, maybe next time it will work
				log.Errorf("Error updating CSV: %s", err)
			}
			now := time.Now()
			// next launch is at 00:10 UTC tomorrow
			// extra 10 minutes is to let prometheus to collect all the data for the previous day
			nextRunAt = time.Date(now.Year(), now.Month(), now.Day(), 0, 10, 0, 0, time.UTC).AddDate(0, 0, 1)
		}
	}
}

func UpdateCSV(ctx context.Context, storage CloudStorage, model AllocationModel, path string) error {
	exporter := &csvExporter{
		Storage:  storage,
		Model:    model,
		FilePath: path,
	}
	return exporter.Update(ctx)
}

type csvExporter struct {
	Storage  CloudStorage
	Model    AllocationModel
	FilePath string
}

// Update updates CSV file in cloud storage with new allocation data
func (e *csvExporter) Update(ctx context.Context) error {
	allocationDates, err := e.availableAllocationDates()
	if err != nil {
		return err
	}

	exist, err := e.Storage.Exists(e.FilePath)
	if err != nil {
		return err
	}

	var result []byte

	// cloud storage doesn't have an existing file
	// dump all the data exist to the file
	if !exist {
		result, err = e.allocationsToCSV(ctx, mapTimeToSlice(allocationDates))
		if err != nil {
			return err
		}
	}

	// existing export file exists
	// scan through it and ignore all dates that are already in the file
	// avoid modifying existing data or producing duplicates
	if exist {
		previousExport, err := e.Storage.Read(e.FilePath)
		if err != nil {
			return err
		}

		csvDates, err := e.loadDates(previousExport)
		if err != nil {
			return err
		}

		for date := range csvDates {
			delete(allocationDates, date)
		}

		dateExport, err := e.allocationsToCSV(ctx, mapTimeToSlice(allocationDates))
		if err != nil {
			return err
		}

		result, err = mergeCSV([][]byte{previousExport, dateExport})
		if err != nil {
			return err
		}
	}

	err = e.Storage.Write(e.FilePath, result)
	if err != nil {
		return err
	}

	return nil
}

func (e *csvExporter) availableAllocationDates() (map[time.Time]struct{}, error) {
	start, end, err := e.Model.DateRange()
	if err != nil {
		return nil, err
	}
	if start != time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, time.UTC) {
		// start doesn't start from 00:00 UTC, it could be truncated by prometheus retention policy
		// skip incomplete data and begin from the day after, otherwise it may corrupt existing data
		start = time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, 1)
	}
	end = time.Date(end.Year(), end.Month(), end.Day(), 0, 0, 0, 0, time.UTC)
	dates := make(map[time.Time]struct{})
	for date := start; date.Before(end); date = date.AddDate(0, 0, 1) {
		dates[date] = struct{}{}
	}
	if len(dates) == 0 {
		return nil, errors.New("no allocation data available")
	}
	return dates, nil
}

func (e *csvExporter) allocationsToCSV(ctx context.Context, dates []time.Time) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := e.writeCSVToWriter(ctx, buf, dates)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (e *csvExporter) writeCSVToWriter(ctx context.Context, w io.Writer, dates []time.Time) error {
	fmtFloat := func(f float64) string {
		return strconv.FormatFloat(f, 'f', -1, 64)
	}
	csvWriter := csv.NewWriter(w)
	// TODO: confirm columns we want to export
	err := csvWriter.Write([]string{
		"Date",
		"Name",
		"CPUCoreUsageAverage",
		"CPUCoreRequestAverage",
		"CPUCost",
		"RAMBytesUsageAverage",
		"RAMBytesRequestAverage",
		"RAMCost",
		"GPUs",
		"GPUCost",
		"NetworkCost",
		"PVBytes",
		"PVCost",
		"TotalCost",
	})
	if err != nil {
		return err
	}
	lines := 0
	for _, date := range dates {
		start := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC)
		end := start.AddDate(0, 0, 1)
		data, err := e.Model.ComputeAllocation(start, end, 5*time.Minute)
		if err != nil {
			return err
		}
		log.Infof("fetched %d records for %s", len(data.Allocations), date.Format("2006-01-02"))
		for _, alloc := range data.Allocations {
			if err := ctx.Err(); err != nil {
				return err
			}

			err := csvWriter.Write([]string{
				date.Format("2006-01-02"),
				alloc.Name,
				fmtFloat(alloc.CPUCoreUsageAverage),
				fmtFloat(alloc.CPUCoreRequestAverage),
				fmtFloat(alloc.CPUTotalCost()),
				fmtFloat(alloc.RAMBytesUsageAverage),
				fmtFloat(alloc.RAMBytesRequestAverage),
				fmtFloat(alloc.RAMTotalCost()),
				fmtFloat(alloc.GPUs()),
				fmtFloat(alloc.GPUCost),
				fmtFloat(alloc.NetworkTotalCost()),
				fmtFloat(alloc.PVBytes()),
				fmtFloat(alloc.PVCost()),
				fmtFloat(alloc.TotalCost()),
			})
			if err != nil {
				return err
			}
			lines++
		}
	}

	if lines == 0 {
		return errNoData
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return err
	}
	log.Infof("exported %d lines", lines)
	return nil
}

// loadDate scans through CSV export file and extract all dates from "Date" column
func (e *csvExporter) loadDates(csvFile []byte) (map[time.Time]struct{}, error) {
	csvReader := csv.NewReader(bytes.NewReader(csvFile))
	header, err := csvReader.Read()
	if err != nil {
		return nil, errors.Wrap(err, "reading csv header")
	}
	dateColIndex := 0
	for i, col := range header {
		if col == "Date" {
			dateColIndex = i
			break
		}
	}
	dates := make(map[time.Time]struct{})
	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, errors.Wrap(err, "reading csv row")
		}
		date, err := time.Parse("2006-01-02", row[dateColIndex])
		if err != nil {
			return nil, errors.Wrap(err, "parsing date")
		}
		dates[date] = struct{}{}
	}
	return dates, nil
}

// mergeCSV merges multiple csv files into one.
// Files may have different headers, but the result will have a header that is a union of all headers.
// The main goal here is to allow changing CSV format without breaking or loosing existing data.
func mergeCSV(files [][]byte) ([]byte, error) {
	var err error
	headers := make([][]string, 0, len(files))
	csvReaders := make([]*csv.Reader, 0, len(files))

	// first, get information about the result header
	for _, file := range files {
		csvReader := csv.NewReader(bytes.NewReader(file))
		header, err := csvReader.Read()
		if errors.Is(err, io.EOF) {
			// ignore empty files
			continue
		}
		if err != nil {
			return nil, errors.Wrap(err, "reading header of csv file")
		}
		headers = append(headers, header)
		csvReaders = append(csvReaders, csvReader)
	}

	mapping, header := combineHeaders(headers)
	output := new(bytes.Buffer)
	csvWriter := csv.NewWriter(output)
	err = csvWriter.Write(mergeHeaders(headers))
	if err != nil {
		return nil, errors.Wrap(err, "writing header to csv file")
	}

	for csvIndex, csvReader := range csvReaders {
		for {
			inputLine, err := csvReader.Read()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return nil, errors.Wrap(err, "reading csv file line")
			}

			outputLine := make([]string, len(header))
			for colIndex := range header {
				destColIndex, ok := mapping[csvIndex][colIndex]
				if !ok {
					continue
				}
				outputLine[destColIndex] = inputLine[colIndex]
			}
			err = csvWriter.Write(outputLine)
			if err != nil {
				return nil, errors.Wrap(err, "writing line to csv file")
			}
		}

	}
	csvWriter.Flush()
	if csvWriter.Error() != nil {
		return nil, errors.Wrapf(csvWriter.Error(), "flushing csv file")
	}

	return output.Bytes(), nil
}

func combineHeaders(headers [][]string) ([]map[int]int, []string) {
	result := make([]string, 0)
	indices := make([]map[int]int, len(headers))
	for i, header := range headers {
		indices[i] = make(map[int]int)
		for j, column := range header {
			if !contains(result, column) {
				result = append(result, column)
				indices[i][j] = len(result) - 1
			} else {
				indices[i][j] = indexOf(result, column)
			}
		}
	}
	return indices, result
}

func mergeHeaders(headers [][]string) []string {
	result := make([]string, 0)
	for _, header := range headers {
		for _, column := range header {
			if !contains(result, column) {
				result = append(result, column)
			}
		}
	}
	return result
}

func contains(slice []string, item string) bool {
	for _, element := range slice {
		if element == item {
			return true
		}
	}
	return false
}

func indexOf(slice []string, element string) int {
	for i, e := range slice {
		if e == element {
			return i
		}
	}
	return -1
}

func mapTimeToSlice(data map[time.Time]struct{}) []time.Time {
	result := make([]time.Time, 0, len(data))
	for key := range data {
		result = append(result, key)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Before(result[j])
	})
	return result
}
