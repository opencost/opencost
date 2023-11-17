package filemanager

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

var ErrNotFound = errors.New("not found")

// FileManager is a unified interface for downloading and uploading files from various storage providers.
type FileManager interface {
	Download(ctx context.Context, f *os.File) error
	Upload(ctx context.Context, f *os.File) error
}

// Examples of valid path:
// - s3://bucket-name/path/to/file.csv
// - gs://bucket-name/path/to/file.csv
// - https://azblobaccount.blob.core.windows.net/containerName/path/to/file.csv
// - alts3://fqdn:port/bucket-name/path/to/file.csv
// - local/file/path.csv

func NewFileManager(filePath string) (FileManager, error) {
	switch {
	case strings.HasPrefix(filePath, "s3://"):
		return NewS3File(filePath)
	case strings.HasPrefix(filePath, "gs://"):
		return NewGCSStorageFile(filePath)
	case strings.Contains(filePath, "blob.core.windows.net"):
		return NewAzureBlobFile(filePath)
	case strings.HasPrefix(filePath, "alts3://"):
		return NewAltS3File(filePath)
	case filePath == "":
		return nil, errors.New("empty path")
	default:
		return NewSystemFile(filePath), nil
	}
}

type AzureBlobFile struct {
	client *blockblob.Client
}

func NewAzureBlobFile(blobURL string) (*AzureBlobFile, error) {
	credential, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, err
	}
	client, err := blockblob.NewClient(blobURL, credential, nil)
	return &AzureBlobFile{client: client}, err
}

func (a *AzureBlobFile) Download(ctx context.Context, f *os.File) error {
	_, err := a.client.DownloadFile(ctx, f, nil)
	// Convert Azure error into our own error.
	var storageErr *azcore.ResponseError
	if errors.As(err, &storageErr) && storageErr.ErrorCode == "BlobNotFound" {
		return ErrNotFound
	}
	return err
}

func (a *AzureBlobFile) Upload(ctx context.Context, f *os.File) error {
	_, err := a.client.UploadFile(ctx, f, nil)
	return err
}

type S3File struct {
	s3Client *s3.Client
	bucket   string
	key      string
}

func NewS3File(filePath string) (*S3File, error) {
	u, err := url.Parse(filePath)
	if err != nil {
		return nil, err
	}

	bucket := u.Host
	key := strings.TrimPrefix(u.Path, "/")

	if bucket == "" || key == "" {
		return nil, fmt.Errorf("invalid s3 path: %s", filePath)
	}

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, err
	}

	return &S3File{
		s3Client: s3.NewFromConfig(cfg),
		bucket:   bucket,
		key:      key,
	}, nil
}

func NewAltS3File(filePath string) (*S3File, error) {
	u, err := url.Parse(filePath)
	if err != nil {
		return nil, err
	}

	clPath := path.Clean(u.Path)

	if len(strings.Split(clPath, "/")) < 3 {
		return nil, fmt.Errorf("invalid s3 path: %s", filePath)
	}

	// Extract bucket and path from url
	bucket, key, _ := strings.Cut(strings.TrimLeft(clPath, "/"), "/")

	if bucket == "" || key == "" {
		return nil, fmt.Errorf("invalid s3 path: %s", filePath)
	}

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, err
	}

	return &S3File{
		s3Client: s3.NewFromConfig(cfg, func(o *s3.Options) {
			// Always use https for the endpoint when using an alternative s3 url.
			// NOTE: From service/s3 v1.38.0 and onwards use EndpointResolverV2 as described in the AWS SDK docs.
			o.EndpointResolver = s3.EndpointResolverFromURL(fmt.Sprintf("https://%v", u.Host), func(e *aws.Endpoint) {
				e.HostnameImmutable = true
			})
		}),
		bucket: bucket, // bucket
		key:    key,    // path/to/file.csv
	}, nil
}

func (c *S3File) Download(ctx context.Context, f *os.File) error {
	_, err := manager.NewDownloader(c.s3Client).Download(ctx, f, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(c.key),
	})

	// Convert AWS error into our own error type.
	var notFound *types.NoSuchKey
	if errors.As(err, &notFound) {
		return ErrNotFound
	}

	return err
}

func (c *S3File) Upload(ctx context.Context, f *os.File) error {
	_, err := manager.NewUploader(c.s3Client).Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(c.key),
		Body:   f,
	})
	return err
}

type GCSStorageFile struct {
	bucket string
	key    string
	client *storage.Client
}

func NewGCSStorageFile(filePath string) (*GCSStorageFile, error) {
	filePath = strings.TrimPrefix(filePath, "gs://")
	parts := strings.SplitN(filePath, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return nil, errors.New("invalid GCS path")
	}

	client, err := storage.NewClient(context.TODO())
	if err != nil {
		return nil, err
	}

	return &GCSStorageFile{
		client: client,
		bucket: parts[0],
		key:    parts[1],
	}, nil
}

func (g *GCSStorageFile) Download(ctx context.Context, f *os.File) error {
	r, err := g.client.Bucket(g.bucket).Object(g.key).NewReader(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return ErrNotFound
		}
		return err
	}
	defer r.Close()
	_, err = io.Copy(f, r)
	return err
}

func (g *GCSStorageFile) Upload(ctx context.Context, f *os.File) error {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}
	w := client.Bucket(g.bucket).Object(g.key).NewWriter(ctx)
	if _, err := io.Copy(w, f); err != nil {
		return err
	}
	return w.Close()
}

func NewSystemFile(filePath string) *SystemFile {
	return &SystemFile{filePath: filePath}
}

type SystemFile struct {
	filePath string
}

func (s *SystemFile) Download(ctx context.Context, f *os.File) error {
	sFile, err := os.Open(s.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return ErrNotFound
		}
		return err
	}
	defer sFile.Close()
	_, err = io.Copy(f, sFile)
	return err
}

func (s *SystemFile) Upload(ctx context.Context, f *os.File) error {
	// we want to avoid truncating the file if the upload fails
	// so want to write to a temp file and then rename it
	// to the final destination
	// temp file should be in the same directory as the final destination
	// to avoid "invalid cross-device link" errors when attempting to rename the file
	_, err := f.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	tmpFilePath := filepath.Join(filepath.Dir(s.filePath), fmt.Sprintf(".tmp-%d", time.Now().UnixNano()))
	tmpF, err := os.Create(tmpFilePath)
	if err != nil {
		return err
	}
	defer os.Remove(tmpF.Name())
	defer tmpF.Close()
	_, err = io.Copy(tmpF, f)
	if err != nil {
		return err
	}
	err = os.Rename(tmpF.Name(), s.filePath)
	if err != nil {
		return err
	}
	return nil
}

type InMemoryFile struct {
	Data []byte
}

func (c *InMemoryFile) Download(ctx context.Context, f *os.File) error {
	if len(c.Data) == 0 {
		return ErrNotFound
	}
	_, err := f.Write(c.Data)
	return err
}

func (c *InMemoryFile) Upload(ctx context.Context, f *os.File) error {
	var err error
	c.Data, err = io.ReadAll(f)
	return err
}
