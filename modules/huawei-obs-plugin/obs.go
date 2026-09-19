package main

import (
	"fmt"

	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
)

// bucketUsage is a bucket's storage footprint at query time.
type bucketUsage struct {
	Name      string
	SizeBytes int64
}

// obsEndpoint builds the regional OBS S3-compatible endpoint for a Huawei Cloud
// region, e.g. "https://obs.la-south-2.myhuaweicloud.com".
func obsEndpoint(region string) string {
	return fmt.Sprintf("https://obs.%s.myhuaweicloud.com", region)
}

// listBucketUsage lists every bucket visible to the given OBS client (optionally
// restricted to bucketFilter, if non-empty) and returns each one's current storage
// size in bytes via GetBucketStorageInfo.
func listBucketUsage(client *obs.ObsClient, bucketFilter map[string]bool) ([]bucketUsage, error) {
	listOut, err := client.ListBuckets(&obs.ListBucketsInput{})
	if err != nil {
		return nil, fmt.Errorf("listing OBS buckets: %w", err)
	}

	var usage []bucketUsage
	for _, bucket := range listOut.Buckets {
		if len(bucketFilter) > 0 && !bucketFilter[bucket.Name] {
			continue
		}
		info, err := client.GetBucketStorageInfo(bucket.Name)
		if err != nil {
			return nil, fmt.Errorf("getting storage info for bucket %s: %w", bucket.Name, err)
		}
		usage = append(usage, bucketUsage{Name: bucket.Name, SizeBytes: info.Size})
	}
	return usage, nil
}
