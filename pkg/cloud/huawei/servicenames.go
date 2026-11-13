package huawei

import "strings"

// ServiceCodeLabel carries the Service Type Code exactly as BSS reports it
// (hws.service.type.ec2), since the CloudCost's Service holds a readable name.
const ServiceCodeLabel = "service_code"

// serviceDisplayNames are the names Huawei Cloud users know each service by
// (mostly the product acronym), keyed by Service Type Code suffix. They are
// what the OpenCost UI and the hub's dashboards show; the code itself stays
// in ServiceCodeLabel.
var serviceDisplayNames = map[string]string{
	"ec2": "ECS", "ecs": "ECS", "bms": "BMS",
	"cce": "CCE", "cci": "CCI",
	"ebs": "EVS", "evs": "EVS",
	"obs": "OBS", "sfs": "SFS", "sfsturbo": "SFS Turbo", "cbr": "CBR",
	"swr": "SWR",
	"rds": "RDS", "dcs": "DCS", "dds": "DDS", "css": "CSS",
	"gaussdb": "GaussDB", "gaussdbformysql": "TaurusDB", "taurusdb": "TaurusDB", "gaussdbfornosql": "GaussDB NoSQL",
	"drs": "DRS", "dws": "DWS", "dli": "DLI", "mrs": "MRS",
	"dms": "DMS (Kafka/RabbitMQ)",
	"kms": "DEW (KMS/CSMS)", "dew": "DEW (KMS/CSMS)", "csms": "DEW (KMS/CSMS)",
	"elb": "ELB", "natgateway": "NAT Gateway", "nat": "NAT Gateway",
	"vpc": "VPC (EIP y ancho de banda)", "eip": "EIP",
	"vpn": "VPN", "er": "Enterprise Router", "dc": "Direct Connect", "directconnect": "Direct Connect",
	"cc": "Cloud Connect", "cdn": "CDN", "vpcep": "VPC Endpoint", "ga": "Global Accelerator",
	"dns": "DNS", "apig": "API Gateway", "waf": "WAF",
	"hss": "HSS", "cfw": "Cloud Firewall", "antiddos": "Anti-DDoS", "aad": "Anti-DDoS", "cnad": "Anti-DDoS",
	"cbh": "CBH", "secmaster": "SecMaster", "scm": "SSL Certificate Manager", "ccm": "Cloud Certificate Manager",
	"dbss":          "DBSS",
	"modelarts":     "ModelArts (MaaS)",
	"functionstage": "FunctionGraph", "functiongraph": "FunctionGraph", "fgs": "FunctionGraph",
	"servicestage": "ServiceStage", "cse": "CSE", "ief": "IEF",
	"lts": "LTS", "ces": "Cloud Eye", "aom": "AOM", "smn": "SMN", "cts": "CTS",
	"rms": "Config", "devcloud": "CodeArts", "codearts": "CodeArts",
	"supportplan": "Support Plan", "organizations": "Organizations",
}

// serviceDisplayName returns the readable name of a BSS CLOUD_SERVICE_TYPE
// value. An unknown Service Type Code shows its suffix in upper case
// ("hws.service.type.foo" -> "FOO"); anything else is returned as is.
func serviceDisplayName(serviceType string) string {
	code, ok := strings.CutPrefix(strings.ToLower(strings.TrimSpace(serviceType)), serviceTypeCodePrefix)
	if !ok {
		return serviceType
	}
	if name, known := serviceDisplayNames[code]; known {
		return name
	}
	return strings.ToUpper(code)
}
