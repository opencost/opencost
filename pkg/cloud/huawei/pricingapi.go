package huawei

import (
	"fmt"

	"github.com/huaweicloud/huaweicloud-sdk-go-v3/core/auth/global"
	"github.com/huaweicloud/huaweicloud-sdk-go-v3/core/auth/provider"
	"github.com/huaweicloud/huaweicloud-sdk-go-v3/core/config"
	bssintl "github.com/huaweicloud/huaweicloud-sdk-go-v3/services/bssintl/v2"
	bssintlmodel "github.com/huaweicloud/huaweicloud-sdk-go-v3/services/bssintl/v2/model"
	bssintlregion "github.com/huaweicloud/huaweicloud-sdk-go-v3/services/bssintl/v2/region"
	"github.com/shopspring/decimal"

	"github.com/opencost/opencost/pkg/cloud/httputil"
	"github.com/opencost/opencost/pkg/env"
)

// bssRegionID selects the BSS endpoint used to price resources. BSS (Billing and
// Subscription Service) is a global service with a small, fixed set of regional
// endpoints (see huaweicloud-sdk-go-v3/services/bssintl/v2/region); it is separate
// from the region the priced resource actually lives in, which is passed per
// product in DemandProductInfo.Region instead. ap-southeast-1 is the international
// (non-mainland-China) BSS endpoint, which is what accounts using region IDs like
// "la-south-2" (Latin America) are registered against.
//
// TODO(Fase 3): if a live account turns out to be registered on the mainland-China
// realm instead, switch to the "github.com/huaweicloud/huaweicloud-sdk-go-v3/services/bss/v2"
// package (cn-north-1 endpoint) rather than bssintl.
const bssRegionID = "ap-southeast-1"

// Product/resource codes for the BSS demandPrice ("on-demand-resources") request.
// ecsCloudServiceType/ecsResourceType/usageMeasureIDHour are confirmed correct
// against a live account (Fase 3): a node demandPrice query with these values
// succeeds. The EVS codes are believed correct per Huawei's published resource_spec
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
	// CCE cluster's StorageClass in Fase 3.
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

	agencyCreds, err := provider.GlobalCredentialMetadataProvider().GetCredentials()
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

// costQueryPageSize is the page size used when paginating the BSS cost-analysed-bills
// query (see fetchCostAnalysedBills). BSS returns one CostDataByDimension row per
// distinct combination of the requested group-by dimensions, not per cost record, so
// this bounds the number of distinct resource/service/region combinations fetched
// per page rather than the number of underlying billing line items.
const costQueryPageSize = 200

// fetchCostAnalysedBills queries the BSS historical cost API (POST
// /v4/costs/cost-analysed-bills/query) for the given time range, grouped by
// resource, cloud service type, and region, paginating through all result pages.
// costType/amountType select which cost figure comes back in Cost.Amount (see
// costintegration.go for how the two are combined with Cost.OfficialAmount to
// populate OpenCost's CloudCost fields).
func fetchCostAnalysedBills(projectID string, beginTime, endTime string, costType, amountType string) ([]bssintlmodel.CostDataByDimension, error) {
	client, err := newBssClient()
	if err != nil {
		return nil, err
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
				Groupby: []bssintlmodel.GroupBy{
					{Type: "dimension", Key: "RESOURCE_ID"},
					{Type: "dimension", Key: "CLOUD_SERVICE_TYPE"},
					{Type: "dimension", Key: "REGION_CODE"},
				},
				CostType:   costType,
				AmountType: amountType,
				Offset:     &offset,
				Limit:      &limit,
			},
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
