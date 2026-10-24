package huawei

import (
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/huaweicloud/huaweicloud-sdk-go-v3/core/auth"
	"github.com/huaweicloud/huaweicloud-sdk-go-v3/core/auth/global"
	"github.com/huaweicloud/huaweicloud-sdk-go-v3/core/auth/provider"
	"github.com/huaweicloud/huaweicloud-sdk-go-v3/core/config"
	"github.com/huaweicloud/huaweicloud-sdk-go-v3/core/sdkerr"
	bssintl "github.com/huaweicloud/huaweicloud-sdk-go-v3/services/bssintl/v2"
	bssintlmodel "github.com/huaweicloud/huaweicloud-sdk-go-v3/services/bssintl/v2/model"
	bssintlregion "github.com/huaweicloud/huaweicloud-sdk-go-v3/services/bssintl/v2/region"
	"github.com/shopspring/decimal"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/pkg/cloud/httputil"
	"github.com/opencost/opencost/pkg/env"
)

// bssRegion returns the BSS endpoint used to price resources, overridable via
// HUAWEICLOUD_BSS_REGION. BSS (Billing and Subscription Service) is a global
// service with a small, fixed set of regional endpoints (see
// huaweicloud-sdk-go-v3/services/bssintl/v2/region); it is separate from the
// region the priced resource actually lives in, which is passed per product in
// DemandProductInfo.Region instead. The default is the international
// (non-mainland-China) endpoint, which is what accounts using region IDs like
// "la-south-2" (Latin America) are registered against.
//
// Accounts registered on the mainland-China realm reach BSS through a different
// SDK package ("services/bss/v2", cn-north-1) rather than a different region on
// this one, so pointing this at a mainland region will not work; such an account
// needs support for that package.
func bssRegion() string {
	return env.GetHuaweiBSSRegion()
}

// Product/resource codes for the BSS demandPrice ("on-demand-resources") request.
// ecsCloudServiceType/ecsResourceType/usageMeasureIDHour are confirmed correct
// against a live account: a node demandPrice query with these values succeeds. The EVS codes are believed correct per Huawei's published resource_spec
// enum (SATA/SAS/GPSSD/SSD) but are not yet confirmed by a successful live query.
const (
	ecsCloudServiceType = "hws.service.type.ec2"
	ecsResourceType     = "hws.resource.type.vm"
	evsCloudServiceType = "hws.service.type.ebs"
	evsResourceType     = "hws.resource.type.volume"

	// elbCloudServiceType/elbResourceType/elbBasicResourceSpec identify a Shared
	// (Basic) Huawei Cloud ELB for the BSS demandPrice request. All three are
	// confirmed against a real Huawei Cloud bill export: a Shared Load Balancer is
	// billed under hws.service.type.elb / hws.resource.type.elbv2 with SKU code
	// elbv2.basic.default, at $0.053/hour in la-south-2.
	elbCloudServiceType  = "hws.service.type.elb"
	elbResourceType      = "hws.resource.type.elbv2"
	elbBasicResourceSpec = "elbv2.basic.default"

	usageFactorDuration = "Duration"
	// usageMeasureIDHour is the usage_measure_id enum value for "hour".
	usageMeasureIDHour = 4
	// sizeMeasureIDGB is the size_measure_id enum value for "GB", used for EVS volumes.
	sizeMeasureIDGB = 17

	// everestDiskVolumeTypeParam is the StorageClass parameter Huawei CCE's "everest"
	// CSI driver (provisioner "everest-csi-provisioner") uses to record the actual EVS
	// volume type (SATA/SAS/GPSSD/SSD) a PV was provisioned with. The StorageClass
	// *name* is an arbitrary cluster-specific Kubernetes name (e.g. "csi-disk-dss")
	// and is not a valid BSS resource_spec on its own. Confirmed against a live
	// CCE cluster's StorageClass.
	everestDiskVolumeTypeParam = "everest.io/disk-volume-type"

	// evsReferenceSizeGB is the volume size (GB) used to query EVS on-demand pricing.
	// BSS returns a total price for the queried size; dividing by this constant
	// yields a GB-hour rate, which assumes Huawei Cloud EVS pricing is linear in
	// size. This matches how EVS is billed in practice (a flat rate per GB).
	evsReferenceSizeGB = 100
)

// evsReferenceSizeGBDecimal is evsReferenceSizeGB as a decimal.Decimal, used to
// derive a GB-hour rate from the BSS demandPrice total for a queried EVS volume.
var evsReferenceSizeGBDecimal = decimal.NewFromInt(evsReferenceSizeGB)

// bssEndpointOverride lets tests point the SDK client at an httptest server instead
// of the real Huawei Cloud endpoint. Production code leaves this empty.
var bssEndpointOverride string

// huaweiGlobalCredentials resolves the global.Credentials used to authenticate
// against BSS. Static credentials (HUAWEICLOUD_ACCESS_KEY_ID/_SECRET_ACCESS_KEY,
// optionally +_DOMAIN_ID) take precedence when both AK and SK are set; otherwise
// it falls back to the IAM agency attached to the node, fetched from the ECS/CCE
// instance metadata service (http://169.254.169.254/openstack/latest/securitykey)
// via the SDK's own MetadataCredentialProvider -- the same mechanism other
// in-cluster tools (e.g. ces-exporter) use, so no static AK/SK need to be
// provisioned at all when the node already has an agency configured.
//
// HUAWEICLOUD_DOMAIN_ID is optional in both cases: GlobalCredentials.
// ProcessAuthParams auto-discovers the domain ID from the credential's own IAM
// permissions (via ListAuthDomains/GetCallerIdentity) when it's left empty.
//
// agencyCredsCacheTTL bounds how long the resolved agency credentials are
// reused before re-fetching from the metadata service. Every call to
// provider.GlobalCredentialMetadataProvider().GetCredentials() builds a brand
// new internal.MetadataAccessor (token cached in memory is per-instance, not
// shared across calls), so without caching at this level every DownloadPricingData
// invocation triggers a fresh round trip to 169.254.169.254 for both the
// metadata token and the security key -- the same flood problem
// huaweiProjectIDFromMetadata (metadata.go) has, and the observed cause of
// "too frequent" 503s from the metadata service. Huawei Cloud agency-assumed temporary
// credentials are valid well beyond this TTL, so re-using them for 10 minutes
// is safe.
const agencyCredsCacheTTL = 10 * time.Minute

var (
	agencyCredsCacheMu    sync.Mutex
	agencyCredsCacheValue *global.Credentials
	agencyCredsCacheAt    time.Time
)

// agencyCredentials fetches the IAM agency's temporary credentials from the
// instance metadata service. Tests replace it so they never reach a real
// metadata service.
var agencyCredentials = func() (auth.ICredential, error) {
	return provider.GlobalCredentialMetadataProvider().GetCredentials()
}

func huaweiGlobalCredentials() (*global.Credentials, error) {
	ak := env.GetHuaweiAccessKeyID()
	sk := env.GetHuaweiAccessKeySecret()
	domainID := env.GetHuaweiDomainID()

	if ak != "" && sk != "" {
		creds, err := global.NewCredentialsBuilder().
			WithAk(ak).
			WithSk(sk).
			WithDomainId(domainID).
			SafeBuild()
		if err != nil {
			return nil, fmt.Errorf("building huawei cloud credentials: %w", err)
		}
		return creds, nil
	}

	agencyCredsCacheMu.Lock()
	if agencyCredsCacheValue != nil && time.Since(agencyCredsCacheAt) < agencyCredsCacheTTL {
		cached := agencyCredsCacheValue
		agencyCredsCacheMu.Unlock()
		return cached, nil
	}
	agencyCredsCacheMu.Unlock()

	agencyCreds, err := agencyCredentials()
	if err != nil {
		return nil, fmt.Errorf(
			"huawei cloud BSS pricing requires either %s/%s to be set, or an IAM agency configured on the node: %w",
			env.HuaweiAccessKeyIDEnvVar, env.HuaweiAccessKeySecretEnvVar, err)
	}
	creds, ok := agencyCreds.(*global.Credentials)
	if !ok {
		return nil, fmt.Errorf("huawei cloud BSS pricing: unexpected credential type %T from IAM agency", agencyCreds)
	}
	if domainID != "" {
		creds.DomainId = domainID
	}

	agencyCredsCacheMu.Lock()
	agencyCredsCacheValue = creds
	agencyCredsCacheAt = time.Now()
	agencyCredsCacheMu.Unlock()

	return creds, nil
}

// newBssClient builds a BSS (international) client authenticated via
// huaweiGlobalCredentials.
func newBssClient() (*bssintl.BssintlClient, error) {
	creds, err := huaweiGlobalCredentials()
	if err != nil {
		return nil, err
	}

	builder := bssintl.BssintlClientBuilder().
		WithCredential(creds).
		WithHttpConfig(config.DefaultHttpConfig().WithTimeout(httputil.PricingTimeout).WithRetries(3))

	if bssEndpointOverride != "" {
		builder = builder.WithEndpoint(bssEndpointOverride)
	} else {
		region, err := bssintlregion.SafeValueOf(bssRegion())
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

// fetchOnDemandRatings queries the BSS demandPrice API (POST
// /v2/bills/ratings/on-demand-resources) for the given products in a single batch
// request and returns the raw response for the caller to match back to products by
// DemandProductInfo.Id.
func fetchOnDemandRatings(projectID string, products []bssintlmodel.DemandProductInfo) (*bssintlmodel.ListOnDemandResourceRatingsResponse, error) {
	client, err := newBssClient()
	if err != nil {
		return nil, err
	}

	req := &bssintlmodel.ListOnDemandResourceRatingsRequest{
		Body: &bssintlmodel.RateOnDemandReq{
			ProjectId:    projectID,
			ProductInfos: products,
		},
	}

	resp, err := client.ListOnDemandResourceRatings(req)
	if err != nil {
		return nil, fmt.Errorf("querying huawei cloud BSS demandPrice: %w", err)
	}
	return resp, nil
}

// maxRatingRequests bounds the demandPrice requests one pricing download makes
// while isolating the products BSS rejects (see fetchOnDemandRatingResults).
// Halving a batch with one rejected product takes about 2*log2(n)+1 requests,
// so this covers several rejected products in a batch of a few hundred.
const maxRatingRequests = 32

// fetchOnDemandRatingResults prices products with as few demandPrice requests
// as it can, without letting one product BSS cannot price (an unsupported
// flavor, a spec not sold in the region) cost the rest of the batch their
// live prices: BSS rejects the whole request for it. A rejected batch is
// halved and each half priced on its own, down to the single rejected
// products, which are left unpriced and keep their static rates. Any other
// failure -- authentication, throttling, an outage -- is returned as is.
func fetchOnDemandRatingResults(projectID string, products []bssintlmodel.DemandProductInfo) ([]bssintlmodel.DemandProductRatingResult, error) {
	budget := maxRatingRequests
	return rateIsolatingRejected(projectID, products, &budget)
}

func rateIsolatingRejected(projectID string, products []bssintlmodel.DemandProductInfo, budget *int) ([]bssintlmodel.DemandProductRatingResult, error) {
	if len(products) == 0 {
		return nil, nil
	}
	if *budget <= 0 {
		log.Warnf("huawei cloud: BSS demandPrice request budget spent, %d products keep their previous or static prices", len(products))
		return nil, nil
	}
	*budget--

	resp, err := fetchOnDemandRatings(projectID, products)
	if err == nil {
		if resp.ProductRatingResults == nil {
			return nil, nil
		}
		return *resp.ProductRatingResults, nil
	}
	if !isProductRejection(err) {
		return nil, err
	}
	if len(products) == 1 {
		p := products[0]
		log.Warnf("huawei cloud: BSS cannot price %s %s in %s, it keeps its previous or static price: %v", p.CloudServiceType, p.ResourceSpec, p.Region, err)
		return nil, nil
	}

	mid := len(products) / 2
	first, err := rateIsolatingRejected(projectID, products[:mid], budget)
	if err != nil {
		return nil, err
	}
	second, err := rateIsolatingRejected(projectID, products[mid:], budget)
	if err != nil {
		return nil, err
	}
	return append(first, second...), nil
}

// isProductRejection reports whether BSS refused a demandPrice request for
// what it asked to price (400/404, e.g. "Product not found"), as opposed to
// who asked or when: authentication, permissions, throttling and server
// errors are not something a smaller batch would fix.
func isProductRejection(err error) bool {
	status := 0
	var byRef *sdkerr.ServiceResponseError
	var byValue sdkerr.ServiceResponseError
	switch {
	case errors.As(err, &byRef):
		status = byRef.StatusCode
	case errors.As(err, &byValue):
		status = byValue.StatusCode
	default:
		return false
	}
	return status == http.StatusBadRequest || status == http.StatusNotFound
}

// costQueryPageSize is the page size used when paginating the BSS cost-analysed-bills
// query (see fetchCostAnalysedBills). BSS returns one CostDataByDimension row per
// distinct combination of the requested group-by dimensions, not per cost record, so
// this bounds the number of distinct resource/service/region combinations fetched
// per page rather than the number of underlying billing line items.
const costQueryPageSize = 200

// costQueryDimensions identifies a billed resource: which resource, of which
// service, in which region.
//
// This is the whole budget: BSS rejects a group-by of more than three
// dimensions ("listCosts.req.groupby: size must be between 1 and 3", error code
// CBC.0100), so descriptive dimensions such as RESOURCE_TYPE and RES_SPEC_CODE
// cannot be requested alongside these. What the resource is comes out of the
// RESOURCE_ID value instead -- BSS packs the service, resource type and name
// into it (see describeResource).
var costQueryDimensions = []string{"RESOURCE_ID", "CLOUD_SERVICE_TYPE", "REGION_CODE"}

// fetchCostAnalysedBills queries the BSS historical cost API (POST
// /v4/costs/cost-analysed-bills/query) for the given time range, grouped by
// resource, cloud service type and region, paginating through all result pages.
// costType/amountType select which cost figure comes back in Cost.Amount (see
// costintegration.go for how the two are combined with Cost.OfficialAmount to
// populate OpenCost's CloudCost fields).
func fetchCostAnalysedBills(beginTime, endTime string, costType, amountType string) ([]bssintlmodel.CostDataByDimension, error) {
	return fetchCostAnalysedBillsBy(costQueryDimensions, beginTime, endTime, costType, amountType)
}

// fetchCostAnalysedBillsBy is fetchCostAnalysedBills grouped by the given
// dimensions and, when filters are given, narrowed to the matching costs on
// the BSS side.
func fetchCostAnalysedBillsBy(dimensions []string, beginTime, endTime string, costType, amountType string, filters ...bssintlmodel.FilterV2) ([]bssintlmodel.CostDataByDimension, error) {
	client, err := newBssClient()
	if err != nil {
		return nil, err
	}

	groupby := make([]bssintlmodel.GroupBy, 0, len(dimensions))
	for _, dimension := range dimensions {
		groupby = append(groupby, bssintlmodel.GroupBy{Type: "dimension", Key: dimension})
	}

	xLanguage := "en_us"
	var allRows []bssintlmodel.CostDataByDimension
	var offset int32 = 0
	limit := int32(costQueryPageSize)

	for {
		req := &bssintlmodel.ListCostsRequest{
			XLanguage: &xLanguage,
			Body: &bssintlmodel.ListCostsReq{
				TimeCondition: &bssintlmodel.TimeCondition{
					TimeMeasureId: 1, // 1 = daily
					BeginTime:     beginTime,
					EndTime:       endTime,
				},
				Groupby:    groupby,
				CostType:   costType,
				AmountType: amountType,
				Offset:     &offset,
				Limit:      &limit,
			},
		}
		if len(filters) > 0 {
			req.Body.Filters = &filters
		}

		resp, err := client.ListCosts(req)
		if err != nil {
			return nil, fmt.Errorf("querying huawei cloud BSS cost-analysed-bills: %w", err)
		}
		if resp.CostData != nil {
			allRows = append(allRows, *resp.CostData...)
		}

		if resp.TotalCount == nil || offset+limit >= *resp.TotalCount || resp.CostData == nil || len(*resp.CostData) == 0 {
			break
		}
		offset += limit
	}

	return allRows, nil
}

func buildNodeProductInfo(id, region, instanceType, kubernetesOS string) bssintlmodel.DemandProductInfo {
	osSuffix := "linux"
	if kubernetesOS == "windows" {
		osSuffix = "win"
	}
	usageValue := decimal.NewFromInt(1)
	return bssintlmodel.DemandProductInfo{
		Id:               id,
		CloudServiceType: ecsCloudServiceType,
		ResourceType:     ecsResourceType,
		ResourceSpec:     fmt.Sprintf("%s.%s", instanceType, osSuffix),
		Region:           region,
		UsageFactor:      usageFactorDuration,
		UsageValue:       &usageValue,
		UsageMeasureId:   usageMeasureIDHour,
		SubscriptionNum:  1,
	}
}

// buildLoadBalancerProductInfo builds the BSS demandPrice request for a region's
// Shared (Basic) ELB on-demand price. Unlike node/volume pricing, this is queried
// once per cluster region rather than per distinct in-cluster resource, since
// Huawei Cloud ELBs are not Kubernetes Node/PV resources the clustercache enumerates.
func buildLoadBalancerProductInfo(id, region string) bssintlmodel.DemandProductInfo {
	usageValue := decimal.NewFromInt(1)
	return bssintlmodel.DemandProductInfo{
		Id:               id,
		CloudServiceType: elbCloudServiceType,
		ResourceType:     elbResourceType,
		ResourceSpec:     elbBasicResourceSpec,
		Region:           region,
		UsageFactor:      usageFactorDuration,
		UsageValue:       &usageValue,
		UsageMeasureId:   usageMeasureIDHour,
		SubscriptionNum:  1,
	}
}

func buildVolumeProductInfo(id, region, volumeType string) bssintlmodel.DemandProductInfo {
	usageValue := decimal.NewFromInt(1)
	size := int32(evsReferenceSizeGB)
	measureID := int32(sizeMeasureIDGB)
	return bssintlmodel.DemandProductInfo{
		Id:               id,
		CloudServiceType: evsCloudServiceType,
		ResourceType:     evsResourceType,
		ResourceSpec:     volumeType,
		Region:           region,
		ResourceSize:     &size,
		SizeMeasureId:    &measureID,
		UsageFactor:      usageFactorDuration,
		UsageValue:       &usageValue,
		UsageMeasureId:   usageMeasureIDHour,
		SubscriptionNum:  1,
	}
}
