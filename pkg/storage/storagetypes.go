package storage

import "strings"

/*
 NOTE: This format is to provide monitoring a simple way to identify the storage type with
 NOTE: minimal changes to the Storage interface. It's not very robust, so use caution if
 NOTE: leveraging this type in other systems.
*/

// StorageType is a string identifier for the type of storage used by a Storage implementation.
// The string format is "backend|provider" where backend is the represents the generic storage
// facility, and the provider is the specific implementation.
type StorageType string

const (
	StorageTypeFile        StorageType = "file"
	StorageTypeBucketS3    StorageType = "bucket|s3"
	StorageTypeBucketGCS   StorageType = "bucket|gcs"
	StorageTypeBucketAzure StorageType = "bucket|azure"
)

// IsFileStorage returns true if the StorageType is a file storage type.
func (st StorageType) IsFileStorage() bool {
	return st.BackendType() == "file"
}

// IsBucketStorage returns true if the StorageType is a bucket storage type.
func (st StorageType) IsBucketStorage() bool {
	return st.BackendType() == "bucket"
}

// BackendType returns the backend type if applicable for the storage type.
func (st StorageType) BackendType() string {
	index := strings.Index(string(st), "|")
	if index > 0 {
		return string(st)[:index]
	}
	return string(st)
}

// ProviderType returns the provider type if applicable for the storage type.
func (st StorageType) ProviderType() string {
	index := strings.Index(string(st), "|")

	if index > 0 && index < len(string(st))-1 {
		return string(st)[index+1:]
	}

	return ""
}
