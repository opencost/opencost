package azure

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/opencost/opencost/pkg/cloud"
	"github.com/opencost/opencost/pkg/log"
)

// StorageConnection provides access to Azure Storage
type StorageConnection struct {
	StorageConfiguration
	ConnectionStatus cloud.ConnectionStatus
}

func (sc *StorageConnection) GetStatus() cloud.ConnectionStatus {
	// initialize status if it has not done so; this can happen if the integration is inactive
	if sc.ConnectionStatus.String() == "" {
		sc.ConnectionStatus = cloud.InitialStatus
	}
	return sc.ConnectionStatus
}

func (sc *StorageConnection) Equals(config cloud.Config) bool {
	thatConfig, ok := config.(*StorageConnection)
	if !ok {
		return false
	}

	return sc.StorageConfiguration.Equals(&thatConfig.StorageConfiguration)
}

// getBlobURLTemplate returns the correct BlobUrl for whichever Cloud storage account is specified by the AzureCloud configuration
// defaults to the Public Cloud template
func (sc *StorageConnection) getBlobURLTemplate() string {
	// Use gov cloud blob url if gov is detected in AzureCloud
	if strings.Contains(strings.ToLower(sc.Cloud), "gov") {
		return "https://%s.blob.core.usgovcloudapi.net/%s"
	}
	// default to Public Cloud template
	return "https://%s.blob.core.windows.net/%s"
}

func (sc *StorageConnection) DownloadBlob(blobName string, client *azblob.Client, ctx context.Context) ([]byte, error) {
	log.Infof("Azure Storage: retrieving blob: %v", blobName)

	downloadResponse, err := client.DownloadStream(ctx, sc.Container, blobName, nil)
	if err != nil {
		return nil, fmt.Errorf("Azure: DownloadBlob: failed to download %w", err)
	}
	// NOTE: automatically retries are performed if the connection fails
	retryReader := downloadResponse.NewRetryReader(ctx, &azblob.RetryReaderOptions{})
	defer retryReader.Close()

	// read the body into a buffer
	downloadedData := bytes.Buffer{}

	_, err = downloadedData.ReadFrom(retryReader)
	if err != nil {
		return nil, fmt.Errorf("Azure: DownloadBlob: failed to read downloaded data %w", err)
	}

	return downloadedData.Bytes(), nil
}

// DownloadBlobToFile downloads the Azure Cloud Billing CSV file to a local file
func (sc *StorageConnection) DownloadBlobToFile(localFilePath string, blobName string, client *azblob.Client, ctx context.Context) error {
	log.Infof("Azure: DownloadBlobToFile: retrieving blob: %v", blobName)

	// Remove existing
	if _, err := os.Stat(localFilePath); err == nil {
		err := os.Remove(localFilePath)
		if err != nil {
			return fmt.Errorf("Azure: DownloadBlobToFile: failed to delete existing file %w", err)
		}
	} else {
		return fmt.Errorf("Azure: DownloadBlobToFile: error checking for file %w", err)
	}

	// Create new
	file, err := os.Create(localFilePath)
	if err != nil {
		return fmt.Errorf("Azure: DownloadBlobToFile: failed to create file %w", err)
	}
	defer file.Close()

	filesize, err := client.DownloadFile(ctx, sc.Container, blobName, file, nil)
	if err != nil {
		return fmt.Errorf("Azure: DownloadBlobToFile: failed to download %w", err)
	}

	log.Infof("Azure: DownloadBlobToFile: retrieved %v of size %d", blobName, filesize)

	return nil
}
