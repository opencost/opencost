package main

import (
	"fmt"
	"os"

	"github.com/huaweicloud/huaweicloud-sdk-go-v3/core/auth/global"
	"github.com/huaweicloud/huaweicloud-sdk-go-v3/core/config"
	bssintl "github.com/huaweicloud/huaweicloud-sdk-go-v3/services/bssintl/v2"
	bssintlmodel "github.com/huaweicloud/huaweicloud-sdk-go-v3/services/bssintl/v2/model"
	bssintlregion "github.com/huaweicloud/huaweicloud-sdk-go-v3/services/bssintl/v2/region"
	"github.com/shopspring/decimal"
)

// Env vars shared with the main OpenCost Huawei Cloud provider (pkg/cloud/huawei):
// this plugin runs as a subprocess launched by OpenCost and inherits its
// environment, so credentials are read the same way rather than duplicated in the
// plugin's on-disk config file.
const (
	envAccessKeyID     = "HUAWEICLOUD_ACCESS_KEY_ID"
	envAccessKeySecret = "HUAWEICLOUD_SECRET_ACCESS_KEY"
	envDomainID        = "HUAWEICLOUD_DOMAIN_ID"
	envProjectID       = "HUAWEICLOUD_PROJECT_ID"
)

// bssRegionID is the BSS (Billing and Subscription Service) endpoint region. BSS is
// global with a small fixed set of endpoints, independent of the region OBS buckets
// actually live in (see pkg/cloud/huawei/pricingapi.go for the equivalent used by
// the main ECS/EVS provider).
const bssRegionID = "ap-southeast-1"

// OBS product/resource codes for the BSS demandPrice request. Confirmed against a
// live account: unlike ECS/EVS, OBS resource_spec is not a free-form instance/volume
// type string -- it's one of a fixed set of per-storage-class/per-redundancy SKU
// codes reported by BSS's "list usage types for a resource type" API
// (GET /v2/products/usage-types?resource_type_code=hws.resource.type.obs), and the
// usage_factor/usage_measure_id pair is "size"/9 (a Huawei-internal "division
// measure" enum value), not the "Duration"/4 (hour) pair EVS uses.
//
// obsResourceSpecStandard3AZ prices OBS Standard storage with 3-AZ redundancy, the
// default Huawei Cloud OBS configuration. Other SKUs exist for 1-AZ redundancy and
// for the Warm (infrequent access) and Cold (archive) storage classes (e.g.
// "warm_ext_3az_size_type1", "cold_ext_1az_size_type1") -- see README.md's
// validation notes for why only Standard/3AZ is priced today.
const (
	obsCloudServiceType        = "hws.service.type.obs"
	obsResourceType            = "hws.resource.type.obs"
	obsResourceSpecStandard3AZ = "stdandard_ext_3az_size_type1"

	obsUsageFactorSize    = "size"
	obsUsageMeasureIDSize = 9
	sizeMeasureIDGB       = 17

	// obsReferenceSizeGB is the bucket size (GB) used to query OBS on-demand
	// pricing; dividing the returned total by this constant yields a GB-hour rate.
	obsReferenceSizeGB = 100
)

var obsReferenceSizeGBDecimal = decimal.NewFromInt(obsReferenceSizeGB)

// bssEndpointOverride lets tests point the SDK client at an httptest server.
var bssEndpointOverride string

func newBssClient() (*bssintl.BssintlClient, error) {
	ak := os.Getenv(envAccessKeyID)
	sk := os.Getenv(envAccessKeySecret)
	domainID := os.Getenv(envDomainID)
	if ak == "" || sk == "" || domainID == "" {
		return nil, fmt.Errorf("huawei cloud BSS pricing requires %s, %s and %s to be set", envAccessKeyID, envAccessKeySecret, envDomainID)
	}

	creds, err := global.NewCredentialsBuilder().
		WithAk(ak).
		WithSk(sk).
		WithDomainId(domainID).
		SafeBuild()
	if err != nil {
		return nil, fmt.Errorf("building huawei cloud credentials: %w", err)
	}

	builder := bssintl.BssintlClientBuilder().
		WithCredential(creds).
		WithHttpConfig(config.DefaultHttpConfig().WithTimeout(30_000_000_000).WithRetries(3))

	if bssEndpointOverride != "" {
		builder = builder.WithEndpoint(bssEndpointOverride)
	} else {
		region, err := bssintlregion.SafeValueOf(bssRegionID)
		if err != nil {
			return nil, fmt.Errorf("resolving huawei cloud BSS region: %w", err)
		}
		builder = builder.WithRegion(region)
	}

	hcClient, err := builder.SafeBuild()
	if err != nil {
		return nil, fmt.Errorf("building huawei cloud BSS client: %w", err)
	}
	return bssintl.NewBssintlClient(hcClient), nil
}

// obsGBHourPrice queries the BSS demandPrice API for OBS standard storage in the
// given region and returns a GB-hour rate and the currency it's denominated in.
func obsGBHourPrice(projectID, region string) (decimal.Decimal, string, error) {
	client, err := newBssClient()
	if err != nil {
		return decimal.Zero, "", err
	}

	size := int32(obsReferenceSizeGB)
	measureID := int32(sizeMeasureIDGB)
	usageValue := decimal.NewFromInt(1)

	req := &bssintlmodel.ListOnDemandResourceRatingsRequest{
		Body: &bssintlmodel.RateOnDemandReq{
			ProjectId: projectID,
			ProductInfos: []bssintlmodel.DemandProductInfo{
				{
					Id:               "obs-0",
					CloudServiceType: obsCloudServiceType,
					ResourceType:     obsResourceType,
					ResourceSpec:     obsResourceSpecStandard3AZ,
					Region:           region,
					ResourceSize:     &size,
					SizeMeasureId:    &measureID,
					UsageFactor:      obsUsageFactorSize,
					UsageValue:       &usageValue,
					UsageMeasureId:   obsUsageMeasureIDSize,
					SubscriptionNum:  1,
				},
			},
		},
	}

	resp, err := client.ListOnDemandResourceRatings(req)
	if err != nil {
		return decimal.Zero, "", fmt.Errorf("querying huawei cloud BSS demandPrice for OBS: %w", err)
	}
	if resp.ProductRatingResults == nil || len(*resp.ProductRatingResults) == 0 {
		return decimal.Zero, "", fmt.Errorf("huawei cloud BSS demandPrice returned no pricing results for OBS")
	}
	result := (*resp.ProductRatingResults)[0]
	if result.Amount == nil {
		return decimal.Zero, "", fmt.Errorf("huawei cloud BSS demandPrice returned no amount for OBS")
	}
	currency := "CNY"
	if resp.Currency != nil && *resp.Currency != "" {
		currency = *resp.Currency
	}
	return result.Amount.Div(obsReferenceSizeGBDecimal), currency, nil
}
