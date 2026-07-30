package azure

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/pkg/cloud"
)

// StorageConnection provides access to Azure Storage
type StorageConnection struct {
	StorageConfiguration
	lock             sync.Mutex
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
	} else if strings.Contains(strings.ToLower(sc.Cloud), "china") {
		// Use China cloud blob url if china is detected in AzureCloud
		return "https://%s.blob.core.chinacloudapi.cn/%s"
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

// StreamBlob returns an io.Reader for the given blob which uses a re-usable double buffer approach to stream directly
// from blob storage.
func (sc *StorageConnection) StreamBlob(blobName string, client *azblob.Client) (*StreamReader, error) {
	return NewStreamReader(client, sc.Container, blobName)
}

// isExistingFileCurrent reports whether the file already on disk is a complete,
// up-to-date copy of the blob and can be reused instead of downloading again.
//
// A modification time newer than the blob's is necessary but not sufficient. A
// download that fails partway (for example ENOSPC) or whose process is killed
// leaves a truncated file behind whose modification time is newer than the
// blob's, so the size is also compared against the blob's ContentLength. When
// the blob reports no ContentLength the size cannot be validated and the check
// falls back to the modification time alone.
func isExistingFileCurrent(fileInfo os.FileInfo, blob container.BlobItem) bool {
	if blob.Properties == nil || blob.Properties.LastModified == nil {
		return false
	}
	if !blob.Properties.LastModified.Before(fileInfo.ModTime()) {
		return false
	}
	if blob.Properties.ContentLength != nil && *blob.Properties.ContentLength != fileInfo.Size() {
		return false
	}
	return true
}

// downloadToFile runs download against a temporary file alongside
// localFilePath and moves it into place only once the download has completed
// and, when expectedSize is known, written the expected number of bytes. It
// returns the number of bytes downloaded.
//
// Downloading via a temporary file means a failure can never leave a partial
// file at localFilePath, and never destroys a previously downloaded copy.
func downloadToFile(localFilePath string, expectedSize *int64, download func(*os.File) (int64, error)) (int64, error) {
	dir := filepath.Dir(localFilePath)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return 0, fmt.Errorf("failed to create directory: %w", err)
	}

	// Create the temporary file in the destination directory so the rename is
	// atomic rather than a cross-filesystem copy.
	fp, err := os.CreateTemp(dir, filepath.Base(localFilePath)+".part-*")
	if err != nil {
		return 0, fmt.Errorf("failed to create file: %w", err)
	}
	tempFilePath := fp.Name()

	// Remove the temporary file unless it has been renamed into place.
	committed := false
	defer func() {
		if committed {
			return
		}
		if rmErr := os.Remove(tempFilePath); rmErr != nil && !os.IsNotExist(rmErr) {
			log.Errorf("CloudCost: Azure: downloadToFile: failed to remove temporary file %s: %s", tempFilePath, rmErr)
		}
	}()

	filesize, err := download(fp)
	if err != nil {
		fp.Close()
		return 0, fmt.Errorf("failed to download: %w", err)
	}

	if expectedSize != nil && filesize != *expectedSize {
		fp.Close()
		return 0, fmt.Errorf("download size mismatch: got %d bytes, expected %d", filesize, *expectedSize)
	}

	// Close before renaming so all data is flushed to the file.
	if err := fp.Close(); err != nil {
		return 0, fmt.Errorf("failed to close file: %w", err)
	}

	if err := os.Rename(tempFilePath, localFilePath); err != nil {
		return 0, fmt.Errorf("failed to move downloaded file into place: %w", err)
	}
	committed = true

	return filesize, nil
}

// DownloadBlobToFile downloads the Azure Billing CSV to a local file
func (sc *StorageConnection) DownloadBlobToFile(localFilePath string, blob container.BlobItem, client *azblob.Client, ctx context.Context) error {
	// Lock to prevent accessing a file which may not be fully downloaded
	sc.lock.Lock()
	defer sc.lock.Unlock()
	blobName := *blob.Name
	// Reuse the local copy only when it is a complete, up-to-date copy of the blob
	if fileInfo, err := os.Stat(localFilePath); err == nil {
		if isExistingFileCurrent(fileInfo, blob) {
			log.Debugf("CloudCost: Azure: DownloadBlobToFile: file %s is more recent than corresponding blob %s", localFilePath, blobName)
			return nil
		}
	}

	// Time out to prevent deadlock on download
	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()

	log.Infof("CloudCost: Azure: DownloadBlobToFile: retrieving blob: %v", blobName)

	var expectedSize *int64
	if blob.Properties != nil {
		expectedSize = blob.Properties.ContentLength
	}

	filesize, err := downloadToFile(localFilePath, expectedSize, func(fp *os.File) (int64, error) {
		return client.DownloadFile(timeoutCtx, sc.Container, blobName, fp, nil)
	})
	if err != nil {
		return fmt.Errorf("CloudCost: Azure: DownloadBlobToFile: %w", err)
	}

	log.Infof("CloudCost: Azure: DownloadBlobToFile: retrieved %v of size %dMB", blobName, filesize/1024/1024)

	return nil
}

// deleteFilesOlderThan7d recursively walks the directory specified and deletes
// files which have not been modified in the last 7 days. Returns a list of
// files deleted.
func (sc *StorageConnection) deleteFilesOlderThan7d(localPath string) ([]string, error) {
	sc.lock.Lock()
	defer sc.lock.Unlock()
	duration := 7 * 24 * time.Hour
	cleaned := []string{}
	errs := []string{}

	if _, err := os.Stat(localPath); err != nil {
		return cleaned, nil // localPath does not exist
	}

	filepath.Walk(localPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			errs = append(errs, err.Error())
			return err
		}

		if time.Since(info.ModTime()) > duration {
			err := os.Remove(path)
			if err != nil {
				errs = append(errs, err.Error())
			}
			cleaned = append(cleaned, path)
		}
		return nil
	})

	if len(errs) == 0 {
		return cleaned, nil
	} else {
		return cleaned, fmt.Errorf("deleteFilesOlderThan7d: %v", errs)
	}
}
