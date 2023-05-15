package azure

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/opencost/opencost/pkg/cloud"
	cloudconfig "github.com/opencost/opencost/pkg/cloud/config"
	"github.com/opencost/opencost/pkg/log"
)

// AzureStorageBillingParser accesses billing data stored in CSV files in Azure Storage
type AzureStorageBillingParser struct {
	StorageConnection
}

func (asbp *AzureStorageBillingParser) Equals(config cloudconfig.Config) bool {
	thatConfig, ok := config.(*AzureStorageBillingParser)
	if !ok {
		return false
	}
	return asbp.StorageConnection.Equals(&thatConfig.StorageConnection)
}

type AzureBillingResultFunc func(*BillingRowValues) error

func (asbp *AzureStorageBillingParser) ParseBillingData(start, end time.Time, resultFn AzureBillingResultFunc) (cloud.ConnectionStatus, error) {
	err := asbp.Validate()
	if err != nil {
		return cloud.InvalidConfiguration, err
	}

	containerURL, err := asbp.getContainer()
	if err != nil {
		return cloud.FailedConnection, err
	}
	ctx := context.Background()
	blobNames, err := asbp.getMostRecentBlobs(start, end, containerURL, ctx)
	if err != nil {
		return cloud.FailedConnection, err
	}
	for _, blobName := range blobNames {
		blobBytes, err2 := asbp.DownloadBlob(blobName, containerURL, ctx)
		if err2 != nil {
			return cloud.FailedConnection, err2
		}
		err2 = asbp.parseCSV(start, end, csv.NewReader(bytes.NewReader(blobBytes)), resultFn)
		if err2 != nil {
			return cloud.ParseError, err2
		}

	}
	return cloud.SuccessfulConnection, nil
}

func (asbp *AzureStorageBillingParser) parseCSV(start, end time.Time, reader *csv.Reader, resultFn AzureBillingResultFunc) error {
	headers, err := reader.Read()
	if err != nil {
		return err
	}
	abp, err := NewBillingParseSchema(headers)
	if err != nil {
		return err
	}
	for {
		var record, err = reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		abv := abp.ParseRow(start, end, record)
		if abv == nil {
			continue
		}

		err = resultFn(abv)
		if err != nil {
			return err
		}
	}
	return nil
}

func (asbp *AzureStorageBillingParser) getMostRecentBlobs(start, end time.Time, containerURL *azblob.ContainerURL, ctx context.Context) ([]string, error) {
	log.Infof("Azure Storage: retrieving most recent reports from: %v - %v", start, end)

	// Get list of month substrings for months contained in the start to end range
	monthStrs, err := asbp.getMonthStrings(start, end)
	if err != nil {
		return nil, err
	}
	mostResentBlobs := make(map[string]azblob.BlobItemInternal)
	for marker := (azblob.Marker{}); marker.NotDone(); {
		// Get a result segment starting with the blob indicated by the current Marker.
		listBlob, err := containerURL.ListBlobsFlatSegment(ctx, marker, azblob.ListBlobsSegmentOptions{})
		if err != nil {
			return nil, err
		}

		// ListBlobs returns the start of the next segment; you MUST use this to get
		// the next segment (after processing the current result segment).
		marker = listBlob.NextMarker

		// Using the list of months strings find the most resent blob for each month in the range
		for _, blobInfo := range listBlob.Segment.BlobItems {
			for _, month := range monthStrs {
				if strings.Contains(blobInfo.Name, month) {
					// If Container Path configuration exists, check if it is in the blobs name
					if asbp.Path != "" && !strings.Contains(blobInfo.Name, asbp.Path) {
						continue
					}

					if prevBlob, ok := mostResentBlobs[month]; ok {
						if prevBlob.Properties.CreationTime.After(*blobInfo.Properties.CreationTime) {
							continue
						}
					}
					mostResentBlobs[month] = blobInfo
				}
			}
		}
	}

	// convert blob names into blob urls and move from map into ordered list of blob names
	var blobNames []string
	for _, month := range monthStrs {
		if blob, ok := mostResentBlobs[month]; ok {
			blobNames = append(blobNames, blob.Name)
		}
	}

	return blobNames, nil
}

func (asbp *AzureStorageBillingParser) getMonthStrings(start, end time.Time) ([]string, error) {
	if start.After(end) {
		return []string{}, fmt.Errorf("start date must be before end date")
	}
	if end.After(time.Now()) {
		end = time.Now()
	}
	var monthStrs []string
	monthStr := asbp.timeToMonthString(start)
	endStr := asbp.timeToMonthString(end)
	monthStrs = append(monthStrs, monthStr)
	currMonth := start.AddDate(0, 0, -start.Day()+1)
	for monthStr != endStr {
		currMonth = currMonth.AddDate(0, 1, 0)
		monthStr = asbp.timeToMonthString(currMonth)
		monthStrs = append(monthStrs, monthStr)
	}

	return monthStrs, nil
}

func (asbp *AzureStorageBillingParser) timeToMonthString(input time.Time) string {
	format := "20060102"
	startOfMonth := input.AddDate(0, 0, -input.Day()+1)
	endOfMonth := input.AddDate(0, 1, -input.Day())
	return startOfMonth.Format(format) + "-" + endOfMonth.Format(format)
}
