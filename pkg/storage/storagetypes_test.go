package storage

import "testing"

func assert(t *testing.T, condition bool, msg string) {
	if !condition {
		t.Error(msg)
	}
}

func TestStorageTypes(t *testing.T) {
	fileType := StorageTypeFile
	s3Type := StorageTypeBucketS3
	gcsType := StorageTypeBucketGCS
	azureType := StorageTypeBucketAzure

	assert(t, fileType.BackendType() == "file", "StorageTypeFile.BackendType() should return 'file'")
	assert(t, s3Type.BackendType() == "bucket", "StorageTypeBucketS3.BackendType() should return 'bucket'")
	assert(t, gcsType.BackendType() == "bucket", "StorageTypeBucketGCS.BackendType() should return 'bucket'")
	assert(t, azureType.BackendType() == "bucket", "StorageTypeBucketAzure.BackendType() should return 'bucket'")

	assert(t, fileType.ProviderType() == "", "StorageTypeFile.ProviderType() should return ''")
	assert(t, s3Type.ProviderType() == "s3", "StorageTypeBucketS3.ProviderType() should return 's3'")
	assert(t, gcsType.ProviderType() == "gcs", "StorageTypeBucketGCS.ProviderType() should return 'gcs'")
	assert(t, azureType.ProviderType() == "azure", "StorageTypeBucketAzure.ProviderType() should return 'azure'")

	assert(t, fileType.IsFileStorage(), "StorageTypeFile.IsFileStorage() should return true")
	assert(t, s3Type.IsBucketStorage(), "StorageTypeBucketS3.IsBucketStorage() should return true")
	assert(t, gcsType.IsBucketStorage(), "StorageTypeBucketGCS.IsBucketStorage() should return true")
	assert(t, azureType.IsBucketStorage(), "StorageTypeBucketAzure.IsBucketStorage() should return true")

}
