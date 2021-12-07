package cloud

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/kubecost/cost-model/pkg/env"
	"net/url"
	"strings"
	"time"
)

type CSVRetriever interface {
	GetCSVReaders(start, end time.Time) ([]*csv.Reader, error)
}

type AzureCSVRetriever struct {
}

func (acr AzureCSVRetriever) GetCSVReaders(start, end time.Time) ([]*csv.Reader, error) {

	containerURL, err := acr.getContainer()
	if err != nil {
		return nil, err
	}
	return acr.getMostRecentFiles(start, end, containerURL)
}

func (acr AzureCSVRetriever) getMostRecentFiles(start, end time.Time, containerURL *azblob.ContainerURL) ([]*csv.Reader, error) {
	ctx := context.Background()
	blobNames, err := acr.getMostResentBlobNames(start, end, ctx, containerURL)
	if err != nil {
		return nil, err
	}
	var readers []*csv.Reader
	for _, blobName := range blobNames {
		blobURL := containerURL.NewBlobURL(blobName)

		downloadResponse, err := blobURL.Download(ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})
		if err != nil {
			return nil, err
		}
		// NOTE: automatically retries are performed if the connection fails
		bodyStream := downloadResponse.Body(azblob.RetryReaderOptions{MaxRetryRequests: 20})

		// read the body into a buffer
		downloadedData := bytes.Buffer{}
		_, err = downloadedData.ReadFrom(bodyStream)
		if err != nil {
			return nil, err
		}
		reader := csv.NewReader(bytes.NewReader(downloadedData.Bytes()))
		readers = append(readers, reader)
	}
	return readers, nil
}

func (acr AzureCSVRetriever) getContainer() (*azblob.ContainerURL, error) {
	accountName := env.Get(env.AzureStorageAccountNameEnvVar, "")
	accountKey := env.Get(env.AzureStorageAccessKeyEnvVar, "")
	containerName := env.Get(env.AzureStorageContainerNameEnvVar, "")
	if accountName == "" || accountKey == "" || containerName == "" {
		return nil, fmt.Errorf("set up Azure storage config to access out of cluster costs")
	}

	// Create a default request pipeline using your storage account name and account key.
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return nil, err
	}

	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	// From the Azure portal, get your storage account blob service URL endpoint.
	URL, _ := url.Parse(
		fmt.Sprintf("https://%s.blob.core.windows.net/%s", accountName, containerName))

	// Create a ContainerURL object that wraps the container URL and a request
	// pipeline to make requests.
	containerURL := azblob.NewContainerURL(*URL, p)
	return &containerURL, nil
}

func (acr AzureCSVRetriever) getMostResentBlobNames(start, end time.Time, ctx context.Context, containerURL *azblob.ContainerURL) ([]string, error) {
	// Get list of month substrings for months contained in the start to end range
	monthStrs, err := acr.getMonthStrings(start, end)
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

	// move the blobs names from map into ordered list of blob names
	var blobNames []string
	for _, month := range monthStrs {
		if blob, ok := mostResentBlobs[month]; ok {
			blobNames = append(blobNames, blob.Name)
		}
	}
	return blobNames, nil
}

func (acr AzureCSVRetriever) getMonthStrings(start, end time.Time) ([]string, error) {
	if end.After(time.Now()) {
		end = time.Now()
	}
	if start.After(end) {
		return []string{}, fmt.Errorf("start date must be before end date")
	}

	var monthStrs []string
	monthStr := acr.timeToMonthString(start)
	endStr := acr.timeToMonthString(end)
	monthStrs = append(monthStrs, monthStr)
	currMonth := start.AddDate(0, 0, -start.Day()+1)
	for monthStr != endStr {
		currMonth = currMonth.AddDate(0, 1, 0)
		monthStr = acr.timeToMonthString(currMonth)
		monthStrs = append(monthStrs, monthStr)
	}

	return monthStrs, nil
}

func (acr AzureCSVRetriever) timeToMonthString(input time.Time) string {
	format := "20060102"
	startOfMonth := input.AddDate(0, 0, -input.Day()+1)
	endOfMonth := input.AddDate(0, 1, -input.Day())
	return startOfMonth.Format(format) + "-" + endOfMonth.Format(format)
}
