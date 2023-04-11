package storage

// Fork from Thanos S3 Bucket support to reuse configuration options
// Licensed under the Apache License 2.0
// https://github.com/thanos-io/thanos/blob/main/pkg/objstore/s3/s3.go

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/opencost/opencost/pkg/log"

	"github.com/Azure/azure-pipeline-go/pipeline"
	blob "github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/go-autorest/autorest/adal"
	"github.com/Azure/go-autorest/autorest/azure/auth"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"gopkg.in/yaml.v2"
)

const (
	azureDefaultEndpoint = "blob.core.windows.net"
)

// Set default retry values to default Azure values. 0 = use Default Azure.
var defaultAzureConfig = AzureConfig{
	PipelineConfig: PipelineConfig{
		MaxTries:      0,
		TryTimeout:    0,
		RetryDelay:    0,
		MaxRetryDelay: 0,
	},
	ReaderConfig: ReaderConfig{
		MaxRetryRequests: 0,
	},
	HTTPConfig: AzureHTTPConfig{
		IdleConnTimeout:       model.Duration(90 * time.Second),
		ResponseHeaderTimeout: model.Duration(2 * time.Minute),
		TLSHandshakeTimeout:   model.Duration(10 * time.Second),
		ExpectContinueTimeout: model.Duration(1 * time.Second),
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,
		MaxConnsPerHost:       0,
		DisableCompression:    false,
	},
}

func init() {
	// Disable `ForceLog` in Azure storage module
	// As the time of this patch, the logging function in the storage module isn't correctly
	// detecting expected REST errors like 404 and so outputs them to syslog along with a stacktrace.
	// https://github.com/Azure/azure-storage-blob-go/issues/214
	//
	// This needs to be done at startup because the underlying variable is not thread safe.
	// https://github.com/Azure/azure-pipeline-go/blob/dc95902f1d32034f8f743ccc6c3f2eb36b84da27/pipeline/core.go#L276-L283
	pipeline.SetForceLogEnabled(false)
}

// AzureConfig Azure storage configuration.
type AzureConfig struct {
	StorageAccountName string          `yaml:"storage_account"`
	StorageAccountKey  string          `yaml:"storage_account_key"`
	ContainerName      string          `yaml:"container"`
	Endpoint           string          `yaml:"endpoint"`
	MaxRetries         int             `yaml:"max_retries"`
	MSIResource        string          `yaml:"msi_resource"`
	UserAssignedID     string          `yaml:"user_assigned_id"`
	PipelineConfig     PipelineConfig  `yaml:"pipeline_config"`
	ReaderConfig       ReaderConfig    `yaml:"reader_config"`
	HTTPConfig         AzureHTTPConfig `yaml:"http_config"`
}

type ReaderConfig struct {
	MaxRetryRequests int `yaml:"max_retry_requests"`
}

type PipelineConfig struct {
	MaxTries      int32          `yaml:"max_tries"`
	TryTimeout    model.Duration `yaml:"try_timeout"`
	RetryDelay    model.Duration `yaml:"retry_delay"`
	MaxRetryDelay model.Duration `yaml:"max_retry_delay"`
}

type AzureHTTPConfig struct {
	IdleConnTimeout       model.Duration `yaml:"idle_conn_timeout"`
	ResponseHeaderTimeout model.Duration `yaml:"response_header_timeout"`
	InsecureSkipVerify    bool           `yaml:"insecure_skip_verify"`

	TLSHandshakeTimeout   model.Duration `yaml:"tls_handshake_timeout"`
	ExpectContinueTimeout model.Duration `yaml:"expect_continue_timeout"`
	MaxIdleConns          int            `yaml:"max_idle_conns"`
	MaxIdleConnsPerHost   int            `yaml:"max_idle_conns_per_host"`
	MaxConnsPerHost       int            `yaml:"max_conns_per_host"`
	DisableCompression    bool           `yaml:"disable_compression"`

	TLSConfig TLSConfig `yaml:"tls_config"`
}

// AzureStorage implements the storage.Storage interface against Azure APIs.
type AzureStorage struct {
	name         string
	containerURL blob.ContainerURL
	config       *AzureConfig
}

// Validate checks to see if any of the config options are set.
func (conf *AzureConfig) validate() error {
	var errMsg []string
	if conf.MSIResource == "" {
		if conf.UserAssignedID == "" {
			if conf.StorageAccountName == "" ||
				conf.StorageAccountKey == "" {
				errMsg = append(errMsg, "invalid Azure storage configuration")
			}
			if conf.StorageAccountName == "" && conf.StorageAccountKey != "" {
				errMsg = append(errMsg, "no Azure storage_account specified while storage_account_key is present in config file; both should be present")
			}
			if conf.StorageAccountName != "" && conf.StorageAccountKey == "" {
				errMsg = append(errMsg, "no Azure storage_account_key specified while storage_account is present in config file; both should be present")
			}
		} else {
			if conf.StorageAccountName == "" {
				errMsg = append(errMsg, "UserAssignedID is configured but storage account name is missing")
			}
			if conf.StorageAccountKey != "" {
				errMsg = append(errMsg, "UserAssignedID is configured but storage account key is used")
			}
		}
	} else {
		if conf.StorageAccountName == "" {
			errMsg = append(errMsg, "MSI resource is configured but storage account name is missing")
		}
		if conf.StorageAccountKey != "" {
			errMsg = append(errMsg, "MSI resource is configured but storage account key is used")
		}
	}

	if conf.ContainerName == "" {
		errMsg = append(errMsg, "no Azure container specified")
	}
	if conf.Endpoint == "" {
		conf.Endpoint = azureDefaultEndpoint
	}

	if conf.PipelineConfig.MaxTries < 0 {
		errMsg = append(errMsg, "The value of max_tries must be greater than or equal to 0 in the config file")
	}

	if conf.ReaderConfig.MaxRetryRequests < 0 {
		errMsg = append(errMsg, "The value of max_retry_requests must be greater than or equal to 0 in the config file")
	}

	if len(errMsg) > 0 {
		return errors.New(strings.Join(errMsg, ", "))
	}

	return nil
}

// parseAzureConfig unmarshals a buffer into a Config with default values.
func parseAzureConfig(conf []byte) (AzureConfig, error) {
	config := defaultAzureConfig
	if err := yaml.UnmarshalStrict(conf, &config); err != nil {
		return AzureConfig{}, err
	}

	// If we don't have config specific retry values but we do have the generic MaxRetries.
	// This is for backwards compatibility but also ease of configuration.
	if config.MaxRetries > 0 {
		if config.PipelineConfig.MaxTries == 0 {
			config.PipelineConfig.MaxTries = int32(config.MaxRetries)
		}
		if config.ReaderConfig.MaxRetryRequests == 0 {
			config.ReaderConfig.MaxRetryRequests = config.MaxRetries
		}
	}

	return config, nil
}

// NewAzureStorage returns a new Storage using the provided Azure config.
func NewAzureStorage(azureConfig []byte) (*AzureStorage, error) {
	log.Debugf("Creating new Azure Bucket Connection")

	conf, err := parseAzureConfig(azureConfig)
	if err != nil {
		return nil, err
	}

	return NewAzureStorageWith(conf)
}

// NewAzureStorageWith returns a new Storage using the provided Azure config struct.
func NewAzureStorageWith(conf AzureConfig) (*AzureStorage, error) {
	if err := conf.validate(); err != nil {
		return nil, err
	}

	ctx := context.Background()
	container, err := createContainer(ctx, conf)
	if err != nil {
		ret, ok := err.(blob.StorageError)
		if !ok {
			return nil, errors.Wrapf(err, "Azure API return unexpected error: %T\n", err)
		}
		if ret.ServiceCode() == "ContainerAlreadyExists" {
			log.Debugf("Getting connection to existing Azure blob container: %s", conf.ContainerName)
			container, err = getContainer(ctx, conf)
			if err != nil {
				return nil, errors.Wrapf(err, "cannot get existing Azure blob container: %s", container)
			}
		} else {
			return nil, errors.Wrapf(err, "error creating Azure blob container: %s", container)
		}
	} else {
		log.Infof("Azure blob container successfully created. Address: %s", container)
	}

	return &AzureStorage{
		name:         conf.ContainerName,
		containerURL: container,
		config:       &conf,
	}, nil
}

// Name returns the bucket name for azure storage.
func (as *AzureStorage) Name() string {
	return as.name
}

// StorageType returns a string identifier for the type of storage used by the implementation.
func (as *AzureStorage) StorageType() StorageType {
	return StorageTypeBucketAzure
}

// FullPath returns the storage working path combined with the path provided
func (as *AzureStorage) FullPath(name string) string {
	name = trimLeading(name)

	return name
}

// Stat returns the StorageStats for the specific path.
func (b *AzureStorage) Stat(name string) (*StorageInfo, error) {
	name = trimLeading(name)
	ctx := context.Background()

	blobURL := getBlobURL(name, b.containerURL)
	props, err := blobURL.GetProperties(ctx, blob.BlobAccessConditions{}, blob.ClientProvidedKeyOptions{})
	if err != nil {
		return nil, err
	}

	return &StorageInfo{
		Name:    trimName(name),
		Size:    props.ContentLength(),
		ModTime: props.LastModified(),
	}, nil
}

// Read uses the relative path of the storage combined with the provided path to
// read the contents.
func (b *AzureStorage) Read(name string) ([]byte, error) {
	name = trimLeading(name)
	ctx := context.Background()

	log.Debugf("AzureStorage::Read(%s)", name)

	reader, err := b.getBlobReader(ctx, name, 0, blob.CountToEnd)
	if err != nil {
		return nil, err
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// Write uses the relative path of the storage combined with the provided path
// to write a new file or overwrite an existing file.
func (b *AzureStorage) Write(name string, data []byte) error {
	name = trimLeading(name)
	ctx := context.Background()

	log.Debugf("AzureStorage::Write(%s)", name)

	blobURL := getBlobURL(name, b.containerURL)
	r := bytes.NewReader(data)
	if _, err := blob.UploadStreamToBlockBlob(ctx, r, blobURL,
		blob.UploadStreamToBlockBlobOptions{
			BufferSize: len(data),
			MaxBuffers: 1,
		},
	); err != nil {
		return errors.Wrapf(err, "cannot upload Azure blob, address: %s", name)
	}
	return nil
}

// Remove uses the relative path of the storage combined with the provided path to
// remove a file from storage permanently.
func (b *AzureStorage) Remove(name string) error {
	name = trimLeading(name)

	log.Debugf("AzureStorage::Remove(%s)", name)
	ctx := context.Background()

	blobURL := getBlobURL(name, b.containerURL)
	if _, err := blobURL.Delete(ctx, blob.DeleteSnapshotsOptionInclude, blob.BlobAccessConditions{}); err != nil {
		return errors.Wrapf(err, "error deleting blob, address: %s", name)
	}
	return nil
}

// Exists uses the relative path of the storage combined with the provided path to
// determine if the file exists.
func (b *AzureStorage) Exists(name string) (bool, error) {
	name = trimLeading(name)
	ctx := context.Background()
	blobURL := getBlobURL(name, b.containerURL)
	if _, err := blobURL.GetProperties(ctx, blob.BlobAccessConditions{}, blob.ClientProvidedKeyOptions{}); err != nil {
		var se blob.StorageError
		if errors.As(err, &se) && se.ServiceCode() == blob.ServiceCodeBlobNotFound {
			return false, nil
		}

		return false, errors.Wrapf(err, "cannot get properties for Azure blob, address: %s", name)
	}

	return true, nil
}

// List uses the relative path of the storage combined with the provided path to return
// storage information for the files.
func (b *AzureStorage) List(path string) ([]*StorageInfo, error) {
	path = trimLeading(path)

	log.Debugf("AzureStorage::List(%s)", path)
	ctx := context.Background()

	// Ensure the object name actually ends with a dir suffix. Otherwise we'll just iterate the
	// object itself as one prefix item.
	if path != "" {
		path = strings.TrimSuffix(path, DirDelim) + DirDelim
	}

	marker := blob.Marker{}
	listOptions := blob.ListBlobsSegmentOptions{Prefix: path}

	var names []string
	for i := 1; ; i++ {
		var blobItems []blob.BlobItemInternal

		list, err := b.containerURL.ListBlobsHierarchySegment(ctx, marker, DirDelim, listOptions)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot list hierarchy blobs with prefix %s (iteration #%d)", path, i)
		}

		marker = list.NextMarker
		blobItems = list.Segment.BlobItems

		for _, blob := range blobItems {
			names = append(names, blob.Name)
		}

		// Continue iterating if we are not done.
		if !marker.NotDone() {
			break
		}

		log.Debugf("Requesting next iteration of listing blobs. Entries: %d, iteration: %d", len(names), i)
	}

	// get the storage information for each blob (really unfortunate we have to do this)
	var lock sync.Mutex
	var stats []*StorageInfo
	var wg sync.WaitGroup
	wg.Add(len(names))

	for i := 0; i < len(names); i++ {
		go func(n string) {
			defer wg.Done()

			stat, err := b.Stat(n)
			if err != nil {
				log.Errorf("Error statting blob %s: %s", n, err)
			} else {
				lock.Lock()
				stats = append(stats, stat)
				lock.Unlock()
			}
		}(names[i])
	}

	wg.Wait()

	return stats, nil
}

func (b *AzureStorage) ListDirectories(path string) ([]*StorageInfo, error) {

	path = trimLeading(path)

	log.Debugf("AzureStorage::ListDirectories(%s)", path)
	ctx := context.Background()

	// Ensure the object name actually ends with a dir suffix. Otherwise we'll just iterate the
	// object itself as one prefix item.
	if path != "" {
		path = strings.TrimSuffix(path, DirDelim) + DirDelim
	}

	marker := blob.Marker{}
	listOptions := blob.ListBlobsSegmentOptions{Prefix: path}

	var stats []*StorageInfo
	for i := 1; ; i++ {
		var blobPrefixes []blob.BlobPrefix

		list, err := b.containerURL.ListBlobsHierarchySegment(ctx, marker, DirDelim, listOptions)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot list hierarchy blobs with prefix %s (iteration #%d)", path, i)
		}

		marker = list.NextMarker
		blobPrefixes = list.Segment.BlobPrefixes

		for _, prefix := range blobPrefixes {
			stats = append(stats, &StorageInfo{
				Name: trimLeading(prefix.Name),
			})
		}

		// Continue iterating if we are not done.
		if !marker.NotDone() {
			break
		}

		log.Debugf("Requesting next iteration of listing blobs. Entries: %d, iteration: %d", len(stats), i)
	}

	return stats, nil
}

func (b *AzureStorage) getBlobReader(ctx context.Context, name string, offset, length int64) (io.ReadCloser, error) {
	log.Debugf("Getting blob: %s, offset: %d, length: %d", name, offset, length)
	if name == "" {
		return nil, errors.New("X-Ms-Error-Code: [EmptyContainerName]")
	}
	exists, err := b.Exists(name)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot get blob reader: %s", name)
	}

	if !exists {
		return nil, errors.New("X-Ms-Error-Code: [BlobNotFound]")
	}

	blobURL := getBlobURL(name, b.containerURL)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot get Azure blob URL, address: %s", name)
	}
	var props *blob.BlobGetPropertiesResponse
	props, err = blobURL.GetProperties(ctx, blob.BlobAccessConditions{}, blob.ClientProvidedKeyOptions{})
	if err != nil {
		return nil, errors.Wrapf(err, "cannot get properties for container: %s", name)
	}

	var size int64
	// If a length is specified and it won't go past the end of the file,
	// then set it as the size.
	if length > 0 && length <= props.ContentLength()-offset {
		size = length
		log.Debugf("set size to length. size: %d, length: %d, offset: %d, name: %s", size, length, offset, name)
	} else {
		size = props.ContentLength() - offset
		log.Debugf("set size to go to EOF. contentlength: %d, size: %d, length: %d, offset: %d, name: %s", props.ContentLength(), size, length, offset, name)
	}

	destBuffer := make([]byte, size)

	if err := blob.DownloadBlobToBuffer(context.Background(), blobURL.BlobURL, offset, size,
		destBuffer, blob.DownloadFromBlobOptions{
			BlockSize:   blob.BlobDefaultDownloadBlockSize,
			Parallelism: uint16(3),
			Progress:    nil,
			RetryReaderOptionsPerBlock: blob.RetryReaderOptions{
				MaxRetryRequests: b.config.ReaderConfig.MaxRetryRequests,
			},
		},
	); err != nil {
		return nil, errors.Wrapf(err, "cannot download blob, address: %s", blobURL.BlobURL)
	}

	return io.NopCloser(bytes.NewReader(destBuffer)), nil
}

func getAzureStorageCredentials(conf AzureConfig) (blob.Credential, error) {
	if conf.MSIResource != "" || conf.UserAssignedID != "" {
		spt, err := getServicePrincipalToken(conf)
		if err != nil {
			return nil, err
		}
		if err := spt.Refresh(); err != nil {
			return nil, err
		}

		return blob.NewTokenCredential(spt.Token().AccessToken, func(tc blob.TokenCredential) time.Duration {
			err := spt.Refresh()
			if err != nil {
				log.Errorf("could not refresh MSI token. err: %s", err)
				// Retry later as the error can be related to API throttling
				return 30 * time.Second
			}
			tc.SetToken(spt.Token().AccessToken)
			return spt.Token().Expires().Sub(time.Now().Add(2 * time.Minute))
		}), nil
	}

	credential, err := blob.NewSharedKeyCredential(conf.StorageAccountName, conf.StorageAccountKey)
	if err != nil {
		return nil, err
	}
	return credential, nil
}

func getServicePrincipalToken(conf AzureConfig) (*adal.ServicePrincipalToken, error) {
	resource := conf.MSIResource
	if resource == "" {
		resource = fmt.Sprintf("https://%s.%s", conf.StorageAccountName, conf.Endpoint)
	}

	msiConfig := auth.MSIConfig{
		Resource: resource,
	}

	if conf.UserAssignedID != "" {
		log.Debugf("using user assigned identity. clientId: %s", conf.UserAssignedID)
		msiConfig.ClientID = conf.UserAssignedID
	} else {
		log.Debugf("using system assigned identity")
	}

	return msiConfig.ServicePrincipalToken()
}

func getContainerURL(ctx context.Context, conf AzureConfig) (blob.ContainerURL, error) {
	credentials, err := getAzureStorageCredentials(conf)

	if err != nil {
		return blob.ContainerURL{}, err
	}

	retryOptions := blob.RetryOptions{
		MaxTries:      conf.PipelineConfig.MaxTries,
		TryTimeout:    time.Duration(conf.PipelineConfig.TryTimeout),
		RetryDelay:    time.Duration(conf.PipelineConfig.RetryDelay),
		MaxRetryDelay: time.Duration(conf.PipelineConfig.MaxRetryDelay),
	}

	if deadline, ok := ctx.Deadline(); ok {
		retryOptions.TryTimeout = time.Until(deadline)
	}

	dt, err := DefaultAzureTransport(conf)
	if err != nil {
		return blob.ContainerURL{}, err
	}
	client := http.Client{
		Transport: dt,
	}

	p := blob.NewPipeline(credentials, blob.PipelineOptions{
		Retry:     retryOptions,
		Telemetry: blob.TelemetryOptions{Value: "Kubecost"},
		RequestLog: blob.RequestLogOptions{
			// Log a warning if an operation takes longer than the specified duration.
			// (-1=no logging; 0=default 3s threshold)
			LogWarningIfTryOverThreshold: -1,
		},
		Log: pipeline.LogOptions{
			ShouldLog: nil,
		},
		HTTPSender: pipeline.FactoryFunc(func(next pipeline.Policy, po *pipeline.PolicyOptions) pipeline.PolicyFunc {
			return func(ctx context.Context, request pipeline.Request) (pipeline.Response, error) {
				resp, err := client.Do(request.WithContext(ctx))

				return pipeline.NewHTTPResponse(resp), err
			}
		}),
	})
	u, err := url.Parse(fmt.Sprintf("https://%s.%s", conf.StorageAccountName, conf.Endpoint))
	if err != nil {
		return blob.ContainerURL{}, err
	}
	service := blob.NewServiceURL(*u, p)

	return service.NewContainerURL(conf.ContainerName), nil
}

func DefaultAzureTransport(config AzureConfig) (*http.Transport, error) {
	tlsConfig, err := NewTLSConfig(&config.HTTPConfig.TLSConfig)
	if err != nil {
		return nil, err
	}

	if config.HTTPConfig.InsecureSkipVerify {
		tlsConfig.InsecureSkipVerify = true
	}
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,

		MaxIdleConns:          config.HTTPConfig.MaxIdleConns,
		MaxIdleConnsPerHost:   config.HTTPConfig.MaxIdleConnsPerHost,
		IdleConnTimeout:       time.Duration(config.HTTPConfig.IdleConnTimeout),
		MaxConnsPerHost:       config.HTTPConfig.MaxConnsPerHost,
		TLSHandshakeTimeout:   time.Duration(config.HTTPConfig.TLSHandshakeTimeout),
		ExpectContinueTimeout: time.Duration(config.HTTPConfig.ExpectContinueTimeout),

		ResponseHeaderTimeout: time.Duration(config.HTTPConfig.ResponseHeaderTimeout),
		DisableCompression:    config.HTTPConfig.DisableCompression,
		TLSClientConfig:       tlsConfig,
	}, nil
}

func getContainer(ctx context.Context, conf AzureConfig) (blob.ContainerURL, error) {
	c, err := getContainerURL(ctx, conf)
	if err != nil {
		return blob.ContainerURL{}, err
	}
	// Getting container properties to check if it exists or not. Returns error which will be parsed further.
	_, err = c.GetProperties(ctx, blob.LeaseAccessConditions{})
	return c, err
}

func createContainer(ctx context.Context, conf AzureConfig) (blob.ContainerURL, error) {
	c, err := getContainerURL(ctx, conf)
	if err != nil {
		return blob.ContainerURL{}, err
	}
	_, err = c.Create(
		ctx,
		blob.Metadata{},
		blob.PublicAccessNone)
	return c, err
}

func getBlobURL(blobName string, c blob.ContainerURL) blob.BlockBlobURL {
	return c.NewBlockBlobURL(blobName)
}
