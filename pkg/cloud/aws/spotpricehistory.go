package aws

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	awsSDK "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2Types "github.com/aws/aws-sdk-go-v2/service/ec2/types"

	"github.com/opencost/opencost/core/pkg/log"
)

// SpotPriceHistoryKey uniquely identifies a spot price lookup by region,
// instance type, and availability zone.
type SpotPriceHistoryKey struct {
	Region           string
	InstanceType     string
	AvailabilityZone string
}

func (key SpotPriceHistoryKey) String() string {
	return fmt.Sprintf("%s/%s/%s", key.Region, key.InstanceType, key.AvailabilityZone)
}

const (
	SpotPriceHistoryCacheAge = 1 * time.Hour
)

// SpotPriceHistoryEntry holds a cached spot price from the DescribeSpotPriceHistory API.
type SpotPriceHistoryEntry struct {
	SpotPrice float64
	Timestamp time.Time

	RetrievedAt time.Time
	Error       error // Negative cache
}

func (spe SpotPriceHistoryEntry) shouldRefresh() bool {
	return time.Since(spe.RetrievedAt) > SpotPriceHistoryCacheAge
}

// SpotPriceHistoryCache provides a thread-safe, on-demand cache for spot prices
// retrieved via the DescribeSpotPriceHistory API. Entries are cached for
// SpotPriceHistoryCacheAge and include negative caching for errors.
type SpotPriceHistoryCache struct {
	cache          map[SpotPriceHistoryKey]*SpotPriceHistoryEntry
	mutex          sync.Mutex
	refreshRunning map[SpotPriceHistoryKey]bool
	refreshCond    *sync.Cond

	fetcher SpotPriceHistoryFetcher
}

func NewSpotPriceHistoryCache(fetcher SpotPriceHistoryFetcher) *SpotPriceHistoryCache {
	cache := &SpotPriceHistoryCache{
		cache:          make(map[SpotPriceHistoryKey]*SpotPriceHistoryEntry),
		refreshRunning: make(map[SpotPriceHistoryKey]bool),

		fetcher: fetcher,
	}
	cache.refreshCond = sync.NewCond(&cache.mutex)
	return cache
}

// GetSpotPrice returns the cached spot price for the given region, instance type,
// and availability zone. If the cache entry is missing or stale, it fetches a
// fresh value from the underlying SpotPriceHistoryFetcher.
func (sph *SpotPriceHistoryCache) GetSpotPrice(region, instanceType, availabilityZone string) (*SpotPriceHistoryEntry, error) {
	key := SpotPriceHistoryKey{
		Region:           region,
		InstanceType:     instanceType,
		AvailabilityZone: availabilityZone,
	}
	sph.mutex.Lock()
	for sph.refreshRunning[key] {
		sph.refreshCond.Wait()
	}
	// Check if we have cached price. If so, return it.
	entry, exists := sph.cache[key]
	if exists && !entry.shouldRefresh() {
		sph.mutex.Unlock()
		return entry, entry.Error
	}
	// Either a cache entry does not exist or it is stale. Refresh it.
	sph.refreshRunning[key] = true
	sph.mutex.Unlock()

	// Ensure refreshRunning is always cleared, even if the fetcher panics.
	defer func() {
		sph.mutex.Lock()
		delete(sph.refreshRunning, key)
		sph.refreshCond.Broadcast()
		sph.mutex.Unlock()
	}()

	// Fetch the entry
	entry, err := sph.fetcher.FetchSpotPrice(key)
	if err != nil || entry == nil {
		// If we fail to fetch or get a nil entry, create a negative cache entry.
		if err == nil {
			err = fmt.Errorf("fetcher returned nil entry for %s", key)
		}
		entry = &SpotPriceHistoryEntry{
			RetrievedAt: time.Now(),
			Error:       err,
		}
	} else {
		// Normalize cache metadata so cache freshness does not depend on
		// the fetcher setting these fields correctly.
		entry.RetrievedAt = time.Now()
		entry.Error = nil
	}

	// Store it into the cache
	sph.mutex.Lock()
	sph.cache[key] = entry
	sph.mutex.Unlock()
	return entry, entry.Error
}

// SpotPriceHistoryFetcher is the interface for fetching spot prices from the
// DescribeSpotPriceHistory API (or a mock for testing).
type SpotPriceHistoryFetcher interface {
	FetchSpotPrice(key SpotPriceHistoryKey) (*SpotPriceHistoryEntry, error)
}

// AWSSpotPriceHistoryFetcher implements SpotPriceHistoryFetcher using the real
// AWS EC2 DescribeSpotPriceHistory API. It maintains a pool of per-region
// EC2 clients.
type AWSSpotPriceHistoryFetcher struct {
	awsConfig       awsSDK.Config
	ec2ClientsMutex sync.Mutex
	ec2Clients      map[string]*ec2.Client
}

func NewAWSSpotPriceHistoryFetcher(awsConfig awsSDK.Config) *AWSSpotPriceHistoryFetcher {
	return &AWSSpotPriceHistoryFetcher{
		awsConfig:  awsConfig,
		ec2Clients: make(map[string]*ec2.Client),
	}
}

func (a *AWSSpotPriceHistoryFetcher) getEC2Client(region string) *ec2.Client {
	a.ec2ClientsMutex.Lock()
	defer a.ec2ClientsMutex.Unlock()
	if client, ok := a.ec2Clients[region]; ok {
		return client
	}
	config := a.awsConfig
	config.Region = region
	client := ec2.NewFromConfig(config)
	a.ec2Clients[region] = client
	return client
}

func (a *AWSSpotPriceHistoryFetcher) FetchSpotPrice(key SpotPriceHistoryKey) (*SpotPriceHistoryEntry, error) {
	log.Debugf("Retrieving spot price history for %s", key)
	client := a.getEC2Client(key.Region)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	input := &ec2.DescribeSpotPriceHistoryInput{
		InstanceTypes:    []ec2Types.InstanceType{ec2Types.InstanceType(key.InstanceType)},
		AvailabilityZone: awsSDK.String(key.AvailabilityZone),
		// Only retrieve Linux/UNIX (Amazon VPC) prices. The non-VPC
		// "Linux/UNIX" variant was for EC2-Classic, which was fully retired in
		// August 2023.
		ProductDescriptions: []string{
			"Linux/UNIX (Amazon VPC)",
		},
		// Only retrieve the latest price.
		MaxResults: awsSDK.Int32(1),
	}

	resp, err := client.DescribeSpotPriceHistory(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("describing spot price history for %s: %w", key, err)
	}
	if len(resp.SpotPriceHistory) == 0 {
		return nil, fmt.Errorf("no spot price history found for %s", key)
	}
	spotPrice := resp.SpotPriceHistory[0]

	if spotPrice.SpotPrice == nil || spotPrice.Timestamp == nil {
		return nil, fmt.Errorf("missing required spot price history data for %s (SpotPrice=%v, Timestamp=%v)", key, spotPrice.SpotPrice, spotPrice.Timestamp)
	}
	price, err := strconv.ParseFloat(*spotPrice.SpotPrice, 64)
	if err != nil {
		return nil, fmt.Errorf("parsing spot price: %w", err)
	}
	return &SpotPriceHistoryEntry{
		SpotPrice:   price,
		Timestamp:   *spotPrice.Timestamp,
		RetrievedAt: time.Now(),
	}, nil
}
