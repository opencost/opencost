package azure

import (
	"bytes"
	"context"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/opencost/opencost/pkg/cloud"
	cloudconfig "github.com/opencost/opencost/pkg/cloud/config"
	"github.com/opencost/opencost/pkg/log"
)

// StorageConnection provides access to Azure Storage
type StorageConnection struct {
	StorageConfiguration
	ConnectionStatus cloud.ConnectionStatus
}

func (sc *StorageConnection) GetStatus() cloud.ConnectionStatus {
	return sc.ConnectionStatus
}

func (sc *StorageConnection) Equals(config cloudconfig.Config) bool {
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
		return nil, err
	}
	// NOTE: automatically retries are performed if the connection fails
	retryReader := downloadResponse.NewRetryReader(ctx, &azblob.RetryReaderOptions{})

	// read the body into a buffer
	downloadedData := bytes.Buffer{}

	_, err = downloadedData.ReadFrom(retryReader)
	if err != nil {
		return nil, err
	}

	err = retryReader.Close()
	if err != nil {
		return nil, err
	}
	return downloadedData.Bytes(), nil
}
