package costmodel

import (
	"context"
	"encoding/csv"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/pkg/errors"

	"github.com/opencost/opencost/pkg/kubecost"
	"github.com/opencost/opencost/pkg/log"
)

type CloudStorage interface {
	FileReplace(ctx context.Context, f *os.File, path string) error
	FileDownload(ctx context.Context, path string) (*os.File, error)
	FileExists(ctx context.Context, path string) (bool, error)
}

type AllocationModel interface {
	ComputeAllocation(start, end time.Time, resolution time.Duration) (*kubecost.AllocationSet, error)
	DateRange(ctx context.Context) (time.Time, time.Time, error)
}

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
			dayBefore := time.Date(nextRunAt.Year(), nextRunAt.Month(), nextRunAt.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -1)
			err := UpdateCSV(ctx, storage, model, path, dayBefore)
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

func UpdateCSV(ctx context.Context, storage CloudStorage, model AllocationModel, path string, date time.Time) error {
	exporter := &csvExporter{
		Storage:  storage,
		Model:    model,
		FilePath: path,
	}
	return exporter.Update(ctx, date)
}

type csvExporter struct {
	Storage  CloudStorage
	Model    AllocationModel
	FilePath string
}

// TODO: logging
func (e *csvExporter) Update(ctx context.Context, date time.Time) error {
	exist, err := e.Storage.FileExists(ctx, e.FilePath)
	if err != nil {
		return err
	}

	dateExport, err := e.writeCSVToFile(ctx, date)
	if err != nil {
		return err
	}
	defer dateExport.Close()

	var result *os.File
	if exist {
		// merge existing file with new data
		previousExport, err := e.Storage.FileDownload(ctx, e.FilePath)
		if err != nil {
			return err
		}
		defer previousExport.Close()

		result, err = os.CreateTemp("", "cost-model-*.csv")
		if err != nil {
			return errors.Wrap(err, "creating temp file")
		}
		err = mergeCSV([]*os.File{previousExport, dateExport}, result)
		if err != nil {
			return err
		}
	} else {
		// no existing file, create a new one
		result = dateExport
	}

	// we just finished writing to the file, so we need to seek to the beginning, so we can read from it
	_, err = result.Seek(0, io.SeekStart)
	if err != nil {
		return errors.Wrap(err, "seeking to the beginning of the csv file")
	}

	err = e.Storage.FileReplace(ctx, result, e.FilePath)
	if err != nil {
		return err
	}

	return nil
}

func (e *csvExporter) writeCSVToFile(ctx context.Context, date time.Time) (*os.File, error) {
	f, err := os.CreateTemp("", "cost-model-*.csv")
	if err != nil {
		return nil, errors.Wrap(err, "creating temp file")
	}

	err = e.writeCSVToWriter(ctx, f, date)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (e *csvExporter) writeCSVToWriter(ctx context.Context, w io.Writer, date time.Time) error {
	start := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 1, 0)
	data, err := e.Model.ComputeAllocation(start, end, 5*time.Minute)
	if err != nil {
		return err
	}
	log.Infof("data: %d", len(data.Allocations))
	csvWriter := csv.NewWriter(w)
	// TODO: confirm columns we want to export
	err = csvWriter.Write([]string{
		"Date",
		"Name",
		"CPUCoreUsageAverage",
		"CPUCoreRequestAverage",
		"CPUCost",
		"RAMBytesUsageAverage",
		"RAMBytesRequestAverage",
		"RAMCost",
	})
	if err != nil {
		return err
	}
	for _, alloc := range data.Allocations {
		if err := ctx.Err(); err != nil {
			return err
		}

		err := csvWriter.Write([]string{
			date.Format("2006-01-02"),
			alloc.Name,
			fmtFloat(alloc.CPUCoreUsageAverage),
			fmtFloat(alloc.CPUCoreRequestAverage),
			fmtFloat(alloc.CPUCost),
			fmtFloat(alloc.RAMBytesUsageAverage),
			fmtFloat(alloc.RAMBytesRequestAverage),
			fmtFloat(alloc.RAMCost),
		})
		if err != nil {
			return err
		}
	}
	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return err
	}

	return nil
}

func fmtFloat(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}

// mergeCSV merges multiple csv files into one.
// Files may have different headers, but the result will have a header that is a union of all headers.
// The main goal here is to allow changing CSV format without breaking or loosing existing data.
func mergeCSV(files []*os.File, output *os.File) error {
	var err error
	headers := make([][]string, 0, len(files))
	csvReaders := make([]*csv.Reader, 0, len(files))

	// first, get information about the result header
	for _, file := range files {
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			return errors.Wrapf(err, "seeking to start of %s", file.Name())
		}
		csvReader := csv.NewReader(file)
		header, err := csvReader.Read()
		if errors.Is(err, io.EOF) {
			// ignore empty files
			continue
		}
		if err != nil {
			return errors.Wrapf(err, "reading header of %s", file.Name())
		}
		headers = append(headers, header)
		csvReaders = append(csvReaders, csvReader)
	}

	mapping, header := combineHeaders(headers)

	csvWriter := csv.NewWriter(output)
	err = csvWriter.Write(mergeHeaders(headers))
	if err != nil {
		return errors.Wrapf(err, "writing header to %s", output.Name())
	}

	for csvIndex, csvReader := range csvReaders {
		for {
			inputLine, err := csvReader.Read()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return errors.Wrap(err, "reading csv file line")
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
				return errors.Wrapf(err, "writing line to %s", output.Name())
			}
		}

	}
	csvWriter.Flush()
	_, err = output.Seek(0, io.SeekStart)
	if err != nil {
		return errors.Wrapf(err, "seeking to start of %s", output.Name())
	}

	return nil
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
