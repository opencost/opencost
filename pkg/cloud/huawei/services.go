package huawei

import (
	"slices"
	"strings"

	"github.com/opencost/opencost/core/pkg/opencost"
)

// Label keys carried by the CloudCost properties this integration produces.
// CloudCostProperties has no field for a billed resource's provider-side type
// or console name, so the BSS values for those travel as labels.
const (
	// ResourceTypeLabel carries the provider's own resource type for the
	// billed resource, e.g. "rds.instance" or "volume".
	ResourceTypeLabel = "resource_type"

	// ResourceNameLabel carries the name the billed resource has in the
	// Huawei Cloud console, e.g. "rds-mlops-mysql". Billing APIs identify a
	// resource by ID, so this is the only human-readable handle a CloudCost
	// built from billing data has.
	ResourceNameLabel = "resource_name"
)

// serviceTypeCodePrefix prefixes every Huawei Cloud "Service Type Code"
// (hws.service.type.ec2, hws.service.type.obs, ...).
const serviceTypeCodePrefix = "hws.service.type."

// service describes how one Huawei Cloud service is categorized by OpenCost.
type service struct {
	category string

	// codes are Service Type Code suffixes (the part after
	// "hws.service.type."). They are only ever matched exactly: they are short
	// enough to appear inside unrelated words -- "ces" is the tail of
	// "Services" -- so substring-matching them miscategorizes costs.
	codes []string

	// names are lower-cased English "Service Type" display names. They are
	// matched exactly first and as substrings second, so that a name carrying
	// a qualifier still resolves.
	names []string
}

// services maps the Huawei Cloud services this integration recognizes onto
// OpenCost asset categories. Anything not listed here is reported as Other.
//
// The entries for ECS, EVS, OBS, RDS, ELB, VPC (Elastic IP bandwidth is billed
// under this service type as a "Fixed Bandwidth" resource, not under its own
// "Elastic IP" service type), NAT Gateway, DEW (whose Service Type Code is
// hws.service.type.kms, not dew/csms), CCE, FunctionGraph, LTS, DMS, APIG, WAF,
// Cloud Eye, SMN, AOM, DNS, CodeArts (hws.service.type.devcloud) and SupportPlan
// are confirmed against a real Huawei Cloud bill export. DCS, SFS, CBR and SWR
// did not appear in that export and remain unconfirmed; their codes and names
// match the documented Huawei Cloud values.
var services = []service{
	// Compute
	{opencost.ComputeCategory, []string{"ec2", "ecs", "bms"}, []string{"elastic cloud server", "bare metal server"}},
	{opencost.ComputeCategory, []string{"cce"}, []string{"cloud container engine"}},
	{opencost.ComputeCategory, []string{"functionstage", "functiongraph", "fgs"}, []string{"functiongraph"}},
	{opencost.ComputeCategory, []string{"dcs"}, []string{"distributed cache service"}},
	{opencost.ComputeCategory, []string{"dms"}, []string{"distributed message service"}},
	{opencost.ComputeCategory, []string{"modelarts"}, []string{"modelarts"}},
	// Storage
	{opencost.StorageCategory, []string{"ebs", "evs"}, []string{"elastic volume service"}},
	{opencost.StorageCategory, []string{"obs"}, []string{"object storage service"}},
	{opencost.StorageCategory, []string{"sfs", "sfsturbo"}, []string{"scalable file service"}},
	{opencost.StorageCategory, []string{"cbr"}, []string{"cloud backup and recovery"}},
	{opencost.StorageCategory, []string{"swr"}, []string{"software repository for container"}},
	{opencost.StorageCategory, []string{"rds"}, []string{"relational database service"}},
	{opencost.StorageCategory, []string{"kms", "dew", "csms"}, []string{"data encryption workshop", "key management service", "cloud secret management service"}},
	// Network
	{opencost.NetworkCategory, []string{"elb"}, []string{"elastic load balance"}},
	{opencost.NetworkCategory, []string{"natgateway", "nat"}, []string{"nat gateway"}},
	{opencost.NetworkCategory, []string{"vpc"}, []string{"virtual private cloud"}},
	{opencost.NetworkCategory, []string{"eip"}, []string{"elastic ip"}},
	{opencost.NetworkCategory, []string{"dns"}, []string{"domain name service"}},
	{opencost.NetworkCategory, []string{"apig"}, []string{"api gateway"}},
	{opencost.NetworkCategory, []string{"waf"}, []string{"web application firewall"}},
	// Management / operations
	{opencost.ManagementCategory, []string{"lts"}, []string{"log tank service"}},
	{opencost.ManagementCategory, []string{"ces"}, []string{"cloud eye"}},
	{opencost.ManagementCategory, []string{"aom"}, []string{"application operations management"}},
	{opencost.ManagementCategory, []string{"smn"}, []string{"simple message notification"}},
	{opencost.ManagementCategory, []string{"rms"}, []string{"config", "resource management service"}},
	{opencost.ManagementCategory, []string{"devcloud", "codearts"}, []string{"codearts"}},
	{opencost.ManagementCategory, []string{"supportplan"}, []string{"supportplan", "support plan"}},
}

// selectHuaweiCategory maps a BSS CLOUD_SERVICE_TYPE dimension value to an
// OpenCost asset category. The value may be either form BSS returns: the
// Service Type Code ("hws.service.type.ec2") or the English display name
// ("Elastic Cloud Server"), the latter when the request asks for
// X-Language: en_us. Unrecognized services are reported as Other.
func selectHuaweiCategory(serviceType string) string {
	if svc, ok := lookupService(serviceType); ok {
		return svc.category
	}
	return opencost.OtherCategory
}

func lookupService(serviceType string) (service, bool) {
	normalized := strings.ToLower(strings.TrimSpace(serviceType))
	if normalized == "" {
		return service{}, false
	}

	// Service Type Code form: the suffix identifies the service exactly, so an
	// unknown suffix is unknown -- don't fall through to name matching.
	if code, ok := strings.CutPrefix(normalized, serviceTypeCodePrefix); ok {
		for _, svc := range services {
			if slices.Contains(svc.codes, code) {
				return svc, true
			}
		}
		return service{}, false
	}

	// An exact display name, or a bare abbreviation ("RDS") -- a code without
	// its prefix. Both are unambiguous, so they win over substring matching.
	for _, svc := range services {
		if slices.Contains(svc.names, normalized) || slices.Contains(svc.codes, normalized) {
			return svc, true
		}
	}

	// Last resort: a display name carrying a qualifier, e.g. "Elastic Load
	// Balance (Shared)".
	for _, svc := range services {
		if slices.ContainsFunc(svc.names, func(name string) bool { return strings.Contains(normalized, name) }) {
			return svc, true
		}
	}

	return service{}, false
}
