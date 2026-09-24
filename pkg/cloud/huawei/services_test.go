package huawei

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/opencost"
)

// TestSelectHuaweiCategory covers both forms BSS returns for its
// CLOUD_SERVICE_TYPE dimension -- the English display name and the Service Type
// Code -- for the services seen in a real bill export, plus the fall-through to
// Other that keeps an unknown service from being miscategorized.
func TestSelectHuaweiCategory(t *testing.T) {
	cases := []struct {
		service string
		want    string
	}{
		// Display names, as returned with X-Language: en_us.
		{"Elastic Cloud Server", opencost.ComputeCategory},
		{"Bare Metal Server", opencost.ComputeCategory},
		{"Cloud Container Engine", opencost.ComputeCategory},
		{"FunctionGraph", opencost.ComputeCategory},
		{"Distributed Cache Service", opencost.ComputeCategory},
		{"Distributed Message Service", opencost.ComputeCategory},
		{"ModelArts", opencost.ComputeCategory},
		{"Elastic Volume Service", opencost.StorageCategory},
		{"Object Storage Service", opencost.StorageCategory},
		{"Scalable File Service", opencost.StorageCategory},
		{"Cloud Backup and Recovery", opencost.StorageCategory},
		{"Relational Database Service", opencost.StorageCategory},
		{"Data Encryption Workshop", opencost.StorageCategory},
		{"Elastic Load Balance", opencost.NetworkCategory},
		{"NAT Gateway", opencost.NetworkCategory},
		{"Virtual Private Cloud", opencost.NetworkCategory},
		{"Domain Name Service", opencost.NetworkCategory},
		{"API Gateway", opencost.NetworkCategory},
		{"Web Application Firewall", opencost.NetworkCategory},
		{"Log Tank Service", opencost.ManagementCategory},
		{"Cloud Eye", opencost.ManagementCategory},
		{"Application Operations Management", opencost.ManagementCategory},
		{"Simple Message Notification", opencost.ManagementCategory},
		{"CodeArts", opencost.ManagementCategory},
		{"SupportPlan", opencost.ManagementCategory},

		// Service Type Codes, as they appear in a bill export.
		{"hws.service.type.ec2", opencost.ComputeCategory},
		{"hws.service.type.ebs", opencost.StorageCategory},
		{"hws.service.type.obs", opencost.StorageCategory},
		{"hws.service.type.rds", opencost.StorageCategory},
		{"hws.service.type.kms", opencost.StorageCategory},
		{"hws.service.type.cce", opencost.ComputeCategory},
		{"hws.service.type.functionstage", opencost.ComputeCategory},
		{"hws.service.type.natgateway", opencost.NetworkCategory},
		{"hws.service.type.devcloud", opencost.ManagementCategory},
		{"hws.service.type.supportplan", opencost.ManagementCategory},
		{"hws.service.type.rms", opencost.ManagementCategory},

		// Bare abbreviations and names carrying a qualifier.
		{"RDS", opencost.StorageCategory},
		{"Elastic Load Balance (Shared)", opencost.NetworkCategory},
		{"  object storage service  ", opencost.StorageCategory},

		// Unknown services, including one whose name ends in "Services" -- a
		// bare substring match on the "ces" code would file it under Cloud Eye.
		{"Some Unrecognized Service", opencost.OtherCategory},
		{"Cloud Professional Services", opencost.OtherCategory},
		{"hws.service.type.notaservice", opencost.OtherCategory},
		{"", opencost.OtherCategory},
	}

	for _, c := range cases {
		if got := selectHuaweiCategory(c.service); got != c.want {
			t.Errorf("selectHuaweiCategory(%q) = %q, want %q", c.service, got, c.want)
		}
	}
}
