package opencost

import (
	"slices"
	"strings"
)

// Well-known label keys carried by Cloud assets. AssetProperties has no field
// for a region, a resource type or a hardware spec, so the billing API's values
// for those travel as labels.
const (
	// AssetRegionLabel is where a Cloud asset carries its cloud region. It
	// deliberately reuses the well-known Kubernetes topology label name that
	// Node assets already carry (node labels reach assets with the "label_"
	// prefix stripped, see source.QueryResult.GetLabels), so that a consumer
	// reading an asset's region finds it under one key for every asset type.
	AssetRegionLabel = "topology_kubernetes_io_region"

	// AssetResourceTypeLabel carries the provider's own resource type for the
	// billed resource, e.g. "rds.instance" or "volume".
	AssetResourceTypeLabel = "resource_type"

	// AssetResourceNameLabel carries the name the billed resource has in the
	// provider's console, e.g. "rds-mlops-mysql". Billing APIs identify a
	// resource by ID, so this is the only human-readable handle an asset built
	// from billing data has.
	AssetResourceNameLabel = "resource_name"

	// AssetResourceSpecLabel carries the provider's spec/SKU code for the
	// billed resource, e.g. "rds.mysql.n1.large.2.ha" -- the cloud-service
	// equivalent of a Node's instance type.
	AssetResourceSpecLabel = "resource_spec"
)

// huaweiServiceTypeCodePrefix prefixes every Huawei Cloud "Service Type Code"
// (hws.service.type.ec2, hws.service.type.obs, ...).
const huaweiServiceTypeCodePrefix = "hws.service.type."

// huaweiService describes how one Huawei Cloud service is reported by OpenCost.
type huaweiService struct {
	assetType AssetType
	category  string

	// codes are Service Type Code suffixes (the part after
	// "hws.service.type."). They are only ever matched exactly: they are short
	// enough to appear inside unrelated words -- "ces" is the tail of
	// "Services" -- so substring-matching them mislabels assets.
	codes []string

	// names are lower-cased English "Service Type" display names. They are
	// matched exactly first and as substrings second, so that a name carrying
	// a qualifier still resolves.
	names []string
}

// huaweiServices enumerates the Huawei Cloud services OpenCost reports as their
// own asset type. Everything not listed here is reported as OtherCloud/Other.
//
// The entries for ECS, EVS, OBS, RDS, ELB, VPC (Elastic IP bandwidth is billed
// under this service type as a "Fixed Bandwidth" resource, not under its own
// "Elastic IP" service type), NAT Gateway, DEW (whose Service Type Code is
// hws.service.type.kms, not dew/csms), CCE, FunctionGraph, LTS, DMS, APIG, WAF,
// Cloud Eye, SMN, AOM, DNS, CodeArts (hws.service.type.devcloud) and SupportPlan
// are confirmed against a real Huawei Cloud bill export. DCS, SFS, CBR and SWR
// did not appear in that export and remain unconfirmed; their codes and names
// match the documented Huawei Cloud values.
var huaweiServices = []huaweiService{
	// Compute
	{ECSCloudAssetType, ComputeCategory, []string{"ec2", "ecs", "bms"}, []string{"elastic cloud server", "bare metal server"}},
	{CCECloudAssetType, ComputeCategory, []string{"cce"}, []string{"cloud container engine"}},
	{FGSCloudAssetType, ComputeCategory, []string{"functionstage", "functiongraph", "fgs"}, []string{"functiongraph"}},
	{DCSCloudAssetType, ComputeCategory, []string{"dcs"}, []string{"distributed cache service"}},
	{DMSCloudAssetType, ComputeCategory, []string{"dms"}, []string{"distributed message service"}},
	{ModelArtsCloudAssetType, ComputeCategory, []string{"modelarts"}, []string{"modelarts"}},

	// Storage
	{EVSCloudAssetType, StorageCategory, []string{"ebs", "evs"}, []string{"elastic volume service"}},
	{OBSCloudAssetType, StorageCategory, []string{"obs"}, []string{"object storage service"}},
	{SFSCloudAssetType, StorageCategory, []string{"sfs", "sfsturbo"}, []string{"scalable file service"}},
	{CBRCloudAssetType, StorageCategory, []string{"cbr"}, []string{"cloud backup and recovery"}},
	{SWRCloudAssetType, StorageCategory, []string{"swr"}, []string{"software repository for container"}},
	{RDSCloudAssetType, StorageCategory, []string{"rds"}, []string{"relational database service"}},
	{DEWCloudAssetType, StorageCategory, []string{"kms", "dew", "csms"}, []string{"data encryption workshop", "key management service", "cloud secret management service"}},

	// Network
	{ELBCloudAssetType, NetworkCategory, []string{"elb"}, []string{"elastic load balance"}},
	{NATCloudAssetType, NetworkCategory, []string{"natgateway", "nat"}, []string{"nat gateway"}},
	{VPCCloudAssetType, NetworkCategory, []string{"vpc"}, []string{"virtual private cloud"}},
	{EIPCloudAssetType, NetworkCategory, []string{"eip"}, []string{"elastic ip"}},
	{DNSCloudAssetType, NetworkCategory, []string{"dns"}, []string{"domain name service"}},
	{APIGCloudAssetType, NetworkCategory, []string{"apig"}, []string{"api gateway"}},
	{WAFCloudAssetType, NetworkCategory, []string{"waf"}, []string{"web application firewall"}},

	// Management / operations
	{LTSCloudAssetType, ManagementCategory, []string{"lts"}, []string{"log tank service"}},
	{CESCloudAssetType, ManagementCategory, []string{"ces"}, []string{"cloud eye"}},
	{AOMCloudAssetType, ManagementCategory, []string{"aom"}, []string{"application operations management"}},
	{SMNCloudAssetType, ManagementCategory, []string{"smn"}, []string{"simple message notification"}},
	{RMSCloudAssetType, ManagementCategory, []string{"rms"}, []string{"config", "resource management service"}},
	{CodeArtsCloudAssetType, ManagementCategory, []string{"devcloud", "codearts"}, []string{"codearts"}},
	{SupportPlanCloudAssetType, ManagementCategory, []string{"supportplan"}, []string{"supportplan", "support plan"}},
}

// HuaweiServiceAssetType maps a Huawei Cloud service identifier to the AssetType
// under which that service's Cloud assets are reported, so that the assets API
// exposes RDS, OBS, CCE, ... as distinct types rather than one Cloud catch-all.
// Unrecognized services get OtherCloudAssetType.
//
// The identifier may be either form BSS returns for its CLOUD_SERVICE_TYPE
// dimension: the Service Type Code ("hws.service.type.ec2") or the English
// display name ("Elastic Cloud Server"), the latter when the request asks for
// X-Language: en_us.
func HuaweiServiceAssetType(service string) AssetType {
	if svc, ok := lookupHuaweiService(service); ok {
		return svc.assetType
	}
	return OtherCloudAssetType
}

// HuaweiServiceCategory maps a Huawei Cloud service identifier (see
// HuaweiServiceAssetType for the accepted forms) to an OpenCost asset category.
// Unrecognized services get OtherCategory.
func HuaweiServiceCategory(service string) string {
	if svc, ok := lookupHuaweiService(service); ok {
		return svc.category
	}
	return OtherCategory
}

func lookupHuaweiService(service string) (huaweiService, bool) {
	normalized := strings.ToLower(strings.TrimSpace(service))
	if normalized == "" {
		return huaweiService{}, false
	}

	// Service Type Code form: the suffix identifies the service exactly, so an
	// unknown suffix is unknown -- don't fall through to name matching.
	if code, ok := strings.CutPrefix(normalized, huaweiServiceTypeCodePrefix); ok {
		for _, svc := range huaweiServices {
			if slices.Contains(svc.codes, code) {
				return svc, true
			}
		}
		return huaweiService{}, false
	}

	for _, svc := range huaweiServices {
		if slices.Contains(svc.names, normalized) {
			return svc, true
		}
	}

	// A bare abbreviation ("RDS") is a code without its prefix.
	for _, svc := range huaweiServices {
		if slices.Contains(svc.codes, normalized) {
			return svc, true
		}
	}

	for _, svc := range huaweiServices {
		for _, name := range svc.names {
			if strings.Contains(normalized, name) {
				return svc, true
			}
		}
	}

	return huaweiService{}, false
}
