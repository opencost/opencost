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

type SpotPriceHistoryEntry struct {
	SpotPrice float64
	Timestamp time.Time

	RetrievedAt time.Time
	Error       error // Negative cache
}

func (spe SpotPriceHistoryEntry) shouldRefresh() bool {
	return time.Since(spe.RetrievedAt) > SpotPriceHistoryCacheAge
}

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

	// Fetch the entry
	entry, err := sph.fetcher.FetchSpotPrice(key)
	if err != nil {
		// If we fail to fetch, create a negative cache entry.
		entry = &SpotPriceHistoryEntry{
			RetrievedAt: time.Now(),
			Error:       err,
		}
	}

	// Store it into the cache
	sph.mutex.Lock()
	sph.cache[key] = entry
	delete(sph.refreshRunning, key)
	sph.refreshCond.Broadcast()
	sph.mutex.Unlock()
	return entry, entry.Error
}

type SpotPriceHistoryFetcher interface {
	FetchSpotPrice(key SpotPriceHistoryKey) (*SpotPriceHistoryEntry, error)
}

func NewAWSSpotPriceHistoryFetcher(awsConfig awsSDK.Config) *AWSSpotPriceHistoryFetcher {
	return &AWSSpotPriceHistoryFetcher{
		awsConfig:  awsConfig,
		ec2Clients: make(map[string]*ec2.Client),
	}
}

type AWSSpotPriceHistoryFetcher struct {
	awsConfig       awsSDK.Config
	ec2ClientsMutex sync.Mutex
	ec2Clients      map[string]*ec2.Client
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
	log.Infof("Retrieving spot price history for %s", key)
	client := a.getEC2Client(key.Region)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	input := &ec2.DescribeSpotPriceHistoryInput{
		InstanceTypes:    []ec2Types.InstanceType{ec2Types.InstanceType(key.InstanceType)},
		AvailabilityZone: awsSDK.String(key.AvailabilityZone),
		// Only retrieve Linux/UNIX (Amazon VPC) prices for now. The non-VPC
		// "Linux/UNIX" variant was for EC2-Classic, which was fully retired in
		// August 2023. In the future, we could add support for other operating
		// systems by expanding SpotPriceHistoryKey.
		ProductDescriptions: []string{
			"Linux/UNIX (Amazon VPC)",
		},
		// Only retrieve the latest price.
		MaxResults: awsSDK.Int32(1),
	}

	// Fetch the price
	resp, err := client.DescribeSpotPriceHistory(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("describing spot price history for %s: %w", key, err)
	}
	if len(resp.SpotPriceHistory) == 0 {
		return nil, fmt.Errorf("no spot price history found for %s", key)
	}
	spotPrice := resp.SpotPriceHistory[0]

	// Parse the entry
	if spotPrice.SpotPrice == nil || spotPrice.Timestamp == nil {
		return nil, fmt.Errorf("missing required spot price history data")
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
