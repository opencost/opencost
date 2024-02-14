package azure

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/pkg/cloud"
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

// DownloadBlob downloads the Azure Billing CSV into a byte slice
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

// DownloadBlobToFile downloads the Azure Billing CSV to a local file
func (sc *StorageConnection) DownloadBlobToFile(localFilePath string, blobName string, client *azblob.Client, ctx context.Context) error {
	// If file exists, don't download it again
	if _, err := os.Stat(localFilePath); err == nil {
		log.DedupedInfof(3, "CloudCost: Azure: DownloadBlobToFile: file %v already exists, not downloading %v", localFilePath, blobName)
		return nil
	}

	// Create filepath
	dir := filepath.Dir(localFilePath)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return fmt.Errorf("CloudCost: Azure: DownloadBlobToFile: failed to create directory %w", err)
	}
	fp, err := os.Create(localFilePath)
	if err != nil {
		return fmt.Errorf("CloudCost: Azure: DownloadBlobToFile: failed to create file %w", err)
	}
	defer fp.Close()

	// Download newest Azure Billing CSV to disk
	log.Infof("CloudCost: Azure: DownloadBlobToFile: retrieving blob: %v", blobName)
	filesize, err := client.DownloadFile(ctx, sc.Container, blobName, fp, nil)
	if err != nil {
		return fmt.Errorf("CloudCost: Azure: DownloadBlobToFile: failed to download %w", err)
	}
	log.Infof("CloudCost: Azure: DownloadBlobToFile: retrieved %v of size %dMB", blobName, filesize/1024/1024)

	return nil
}
