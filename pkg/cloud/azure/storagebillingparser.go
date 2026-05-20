package azure

import (
	"compress/gzip"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
	"github.com/opencost/opencost/pkg/cloud"
	"github.com/opencost/opencost/pkg/env"
)

// AzureStorageBillingParser accesses billing data stored in CSV files in Azure Storage
type AzureStorageBillingParser struct {
	StorageConnection
}

func (asbp *AzureStorageBillingParser) Equals(config cloud.Config) bool {
	thatConfig, ok := config.(*AzureStorageBillingParser)
	if !ok {
		return false
	}
	return asbp.StorageConnection.Equals(&thatConfig.StorageConnection)
}

// decompressIfGzipped wraps the reader with a gzip reader if the blob name indicates
// the file is gzip compressed. Returns the original reader if not compressed.
func decompressIfGzipped(r io.Reader, blobName string) (io.ReadCloser, error) {
	if strings.HasSuffix(strings.ToLower(blobName), ".gz") {
		gr, err := gzip.NewReader(r)
		if err != nil {
			return nil, fmt.Errorf("failed to create gzip reader for %s: %w", blobName, err)
		}
		return gr, nil
	}
	// Return a NopCloser to maintain consistent interface
	return io.NopCloser(r), nil
}

// processLocalBillingFile reads a local billing file, decompresses if needed, and parses it
func (asbp *AzureStorageBillingParser) processLocalBillingFile(localFilePath, blobName string, start, end time.Time, resultFn AzureBillingResultFunc) error {
	fp, err := os.Open(localFilePath)
	if err != nil {
		return err
	}
	defer fp.Close()

	// Wrap with gzip reader if needed
	reader, err := decompressIfGzipped(fp, blobName)
	if err != nil {
		return err
	}
	defer reader.Close()

	err = asbp.parseCSV(start, end, csv.NewReader(reader), resultFn)
	if err != nil {
		return err
	}
	return nil
}

// processStreamBillingData reads streaming billing data, decompresses if needed, and parses it
func (asbp *AzureStorageBillingParser) processStreamBillingData(streamReader io.Reader, blobName string, start, end time.Time, resultFn AzureBillingResultFunc) error {
	// Wrap with gzip reader if needed
	reader, err := decompressIfGzipped(streamReader, blobName)
	if err != nil {
		return err
	}
	defer reader.Close()

	err = asbp.parseCSV(start, end, csv.NewReader(reader), resultFn)
	if err != nil {
		return err
	}
	return nil
}

type AzureBillingResultFunc func(*BillingRowValues) error

func (asbp *AzureStorageBillingParser) ParseBillingData(start, end time.Time, resultFn AzureBillingResultFunc) error {
	err := asbp.Validate()
	if err != nil {
		asbp.ConnectionStatus = cloud.InvalidConfiguration
		return err
	}

	// most recent blob list contains information on blob including name and lastMod time
	// Example blobNames: [ export/myExport/20240101-20240131/myExport_758a42af-0731-4edb-b498-1e523bb40f12.csv ]
	ctx := context.Background()
	blobInfos, err := asbp.getBlobInfos(ctx, start, end)
	if err != nil {
		asbp.ConnectionStatus = cloud.FailedConnection
		return err
	}

	if len(blobInfos) == 0 && asbp.ConnectionStatus != cloud.SuccessfulConnection {
		asbp.ConnectionStatus = cloud.MissingData
		return nil
	}

	client, err := asbp.getClient()
	if err != nil {
		asbp.ConnectionStatus = cloud.FailedConnection
		return err
	}

	if env.IsAzureDownloadBillingDataToDisk() {
		// clean up old files that have been saved to disk before downloading new ones
		localPath := env.GetAzureDownloadBillingDataPath()
		if _, err := asbp.deleteFilesOlderThan7d(localPath); err != nil {
			log.Warnf("CloudCost: Azure: ParseBillingData: failed to remove the following stale files: %v", err)
		}
		for _, blob := range blobInfos {
			blobName := *blob.Name

			// Use entire blob name to prevent collision with other files from previous months or other integrations (ex "part_0_0001.csv")
			localFilePath := filepath.Join(localPath, strings.ReplaceAll(blobName, "/", "_"))

			err := asbp.DownloadBlobToFile(localFilePath, blob, client, ctx)
			if err != nil {
				asbp.ConnectionStatus = cloud.FailedConnection
				return err
			}

			err = asbp.processLocalBillingFile(localFilePath, blobName, start, end, resultFn)
			if err != nil {
				asbp.ConnectionStatus = cloud.ParseError
				return err
			}
		}
	} else {
		for _, blobInfo := range blobInfos {
			blobName := *blobInfo.Name
			streamReader, err := asbp.StreamBlob(blobName, client)
			if err != nil {
				asbp.ConnectionStatus = cloud.FailedConnection
				return err
			}

			err = asbp.processStreamBillingData(streamReader, blobName, start, end, resultFn)
			if err != nil {
				asbp.ConnectionStatus = cloud.ParseError
				return err
			}
		}
	}

	asbp.ConnectionStatus = cloud.SuccessfulConnection
	return nil
}

func (asbp *AzureStorageBillingParser) getClient() (*azblob.Client, error) {
	serviceURL := fmt.Sprintf(asbp.StorageConnection.getBlobURLTemplate(), asbp.Account, "")
	client, err := asbp.Authorizer.GetBlobClient(serviceURL)
	if err != nil {
		return nil, err
	}
	return client, nil
}

func (asbp *AzureStorageBillingParser) getBlobInfos(ctx context.Context, start, end time.Time) ([]container.BlobItem, error) {
	client, err := asbp.getClient()
	if err != nil {
		return nil, err
	}
	blobInfos, err := asbp.getMostRecentBlobs(start, end, client, ctx)
	if err != nil {
		return nil, err
	}
	return blobInfos, nil
}

func (asbp *AzureStorageBillingParser) RefreshStatus() cloud.ConnectionStatus {
	end := time.Now().UTC().Truncate(timeutil.Day)
	start := end.Add(-7 * timeutil.Day)

	ctx := context.Background()
	blobInfos, err := asbp.getBlobInfos(ctx, start, end)
	if err != nil {
		asbp.ConnectionStatus = cloud.FailedConnection
	} else if len(blobInfos) == 0 {
		asbp.ConnectionStatus = cloud.MissingData
	} else {
		asbp.ConnectionStatus = cloud.SuccessfulConnection
	}

	return asbp.ConnectionStatus
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

// getMostRecentBlobs returns a list of blobs in the Azure Storage
// Container. It uses the "Last Modified Time" of the file to determine which
// has the latest month-to-date billing data.
func (asbp *AzureStorageBillingParser) getMostRecentBlobs(start, end time.Time, client *azblob.Client, ctx context.Context) ([]container.BlobItem, error) {
	log.Infof("Azure Storage: retrieving most recent reports from: %v - %v", start, end)

	// Get list of month substrings for months contained in the start to end range
	monthStrs, err := asbp.getMonthStrings(start, end)
	if err != nil {
		return nil, err
	}

	// Build map of blobs keyed by month string and blob name
	blobsForMonth := make(map[string]map[string]container.BlobItem)

	pager := client.NewListBlobsFlatPager(asbp.Container, &azblob.ListBlobsFlatOptions{
		Include: container.ListBlobsInclude{Deleted: false, Versions: false},
	})

	for pager.More() {
		resp, err := pager.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		// Using the list of months strings find the most resent blob for each month in the range
		for _, blobInfo := range resp.Segment.BlobItems {
			if blobInfo.Name == nil {
				continue
			}
			// If Container Path configuration exists, check if it is in the blobs name
			if asbp.Path != "" && !strings.Contains(*blobInfo.Name, asbp.Path) {
				continue
			}
			for _, month := range monthStrs {
				if strings.Contains(*blobInfo.Name, month) {
					if _, ok := blobsForMonth[month]; !ok {
						blobsForMonth[month] = make(map[string]container.BlobItem)
					}
					blobsForMonth[month][*blobInfo.Name] = *blobInfo
				}
			}
		}
	}

	// build list of most recent blobs that are needed to fulfil a query on the give date range
	var blobs []container.BlobItem
	for _, monthBlobs := range blobsForMonth {
		// Find most recent blob
		var mostRecentBlob *container.BlobItem
		var mostRecentManifest *container.BlobItem

		for name := range monthBlobs {
			blob := monthBlobs[name]
			lastMod := *blob.Properties.LastModified
			// Handle manifest files
			if strings.HasSuffix(*blob.Name, "manifest.json") {
				if mostRecentManifest == nil {
					mostRecentManifest = &blob

					continue
				}
				if mostRecentManifest.Properties.LastModified.Before(lastMod) {
					mostRecentManifest = &blob
				}
				// Only look at non-manifest blobs if manifests are not present
			} else if mostRecentManifest == nil {
				if mostRecentBlob == nil {
					mostRecentBlob = &blob
					continue
				}
				if mostRecentBlob.Properties.LastModified.Before(lastMod) {
					mostRecentBlob = &blob
				}
			}
		}

		// In the absence of a manifest, add the most recent blob
		if mostRecentManifest == nil {
			if mostRecentBlob != nil {
				blobs = append(blobs, *mostRecentBlob)
			}
			continue
		}

		// download manifest for the month
		manifestBytes, err := asbp.DownloadBlob(*mostRecentManifest.Name, client, ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve manifest %w", err)
		}

		var manifest manifestJson
		err = json.Unmarshal(manifestBytes, &manifest)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal manifest %w", err)
		}

		// Add all partitioned blobs named in the manifest to the list of blobs to be retrieved
		for _, mb := range manifest.Blobs {
			namedBlob, ok := monthBlobs[mb.BlobName]
			if !ok {
				log.Errorf("AzureStorage: failed to find blob named in manifest '%s'", mb.BlobName)
				continue
			}
			blobs = append(blobs, namedBlob)
		}
	}

	return blobs, nil
}

// manifestJson is a struct for unmarshalling manifest.json files associated with the azure billing export
type manifestJson struct {
	Blobs []manifestBlob `json:"blobs"`
}

type manifestBlob struct {
	BlobName string `json:"blobName"`
}

// getMonthStrings returns a list of month strings in the format
// "YYYYMMDD-YYYYMMDD", where the dates are exactly the first and last day of
// the month. It includes all month strings which would capture the start and
// end parameters.
// For example: ["20240201-20240229", "20240101-20240131", "20231201-20231231"]
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
