package opencost

import "testing"

// TestHuaweiServiceAssetType covers both forms BSS returns for its
// CLOUD_SERVICE_TYPE dimension -- the English display name and the Service Type
// Code -- for every service seen in a real bill export, plus the fall-through
// to OtherCloud that keeps an unknown service from being mislabelled.
func TestHuaweiServiceAssetType(t *testing.T) {
	cases := []struct {
		service string
		want    AssetType
	}{
		// Display names, as returned with X-Language: en_us.
		{"Elastic Cloud Server", ECSCloudAssetType},
		{"Bare Metal Server", ECSCloudAssetType},
		{"Cloud Container Engine", CCECloudAssetType},
		{"FunctionGraph", FGSCloudAssetType},
		{"Distributed Cache Service", DCSCloudAssetType},
		{"Distributed Message Service", DMSCloudAssetType},
		{"Elastic Volume Service", EVSCloudAssetType},
		{"Object Storage Service", OBSCloudAssetType},
		{"Relational Database Service", RDSCloudAssetType},
		{"Data Encryption Workshop", DEWCloudAssetType},
		{"Elastic Load Balance", ELBCloudAssetType},
		{"NAT Gateway", NATCloudAssetType},
		{"Virtual Private Cloud", VPCCloudAssetType},
		{"Domain Name Service", DNSCloudAssetType},
		{"API Gateway", APIGCloudAssetType},
		{"Web Application Firewall", WAFCloudAssetType},
		{"Log Tank Service", LTSCloudAssetType},
		{"Cloud Eye", CESCloudAssetType},
		{"Application Operations Management", AOMCloudAssetType},
		{"Simple Message Notification", SMNCloudAssetType},
		{"CodeArts", CodeArtsCloudAssetType},
		{"SupportPlan", SupportPlanCloudAssetType},
		{"ModelArts", ModelArtsCloudAssetType},

		// Service Type Codes, as they appear in a bill export.
		{"hws.service.type.ec2", ECSCloudAssetType},
		{"hws.service.type.ebs", EVSCloudAssetType},
		{"hws.service.type.obs", OBSCloudAssetType},
		{"hws.service.type.rds", RDSCloudAssetType},
		{"hws.service.type.kms", DEWCloudAssetType},
		{"hws.service.type.cce", CCECloudAssetType},
		{"hws.service.type.functionstage", FGSCloudAssetType},
		{"hws.service.type.natgateway", NATCloudAssetType},
		{"hws.service.type.devcloud", CodeArtsCloudAssetType},
		{"hws.service.type.supportplan", SupportPlanCloudAssetType},
		{"hws.service.type.modelarts", ModelArtsCloudAssetType},
		{"hws.service.type.rms", RMSCloudAssetType},

		// Bare abbreviations and names carrying a qualifier.
		{"RDS", RDSCloudAssetType},
		{"Elastic Load Balance (Shared)", ELBCloudAssetType},
		{"  object storage service  ", OBSCloudAssetType},

		// Unknown services, including one whose name ends in "Services" -- a
		// bare substring match on the "ces" code would file it under Cloud Eye.
		{"Some Unrecognized Service", OtherCloudAssetType},
		{"Cloud Professional Services", OtherCloudAssetType},
		{"hws.service.type.notaservice", OtherCloudAssetType},
		{"", OtherCloudAssetType},
	}

	for _, c := range cases {
		if got := HuaweiServiceAssetType(c.service); got != c.want {
			t.Errorf("HuaweiServiceAssetType(%q) = %s, want %s", c.service, got, c.want)
		}
	}
}

func TestHuaweiServiceCategory(t *testing.T) {
	cases := []struct {
		service string
		want    string
	}{
		{"Elastic Cloud Server", ComputeCategory},
		{"Cloud Container Engine", ComputeCategory},
		{"Distributed Cache Service", ComputeCategory},
		{"Elastic Volume Service", StorageCategory},
		{"Object Storage Service", StorageCategory},
		{"Relational Database Service", StorageCategory},
		{"Data Encryption Workshop", StorageCategory},
		{"Elastic Load Balance", NetworkCategory},
		{"Virtual Private Cloud", NetworkCategory},
		{"Web Application Firewall", NetworkCategory},
		{"Log Tank Service", ManagementCategory},
		{"SupportPlan", ManagementCategory},
		{"Some Unrecognized Service", OtherCategory},
	}

	for _, c := range cases {
		if got := HuaweiServiceCategory(c.service); got != c.want {
			t.Errorf("HuaweiServiceCategory(%q) = %q, want %q", c.service, got, c.want)
		}
	}
}

// TestAssetTypeStringsMatchParse guards the AssetType string table against the
// enum drifting out of sync with it: String() indexes the table by ordinal, so
// an entry inserted in the wrong place renames every type after it.
func TestAssetTypeStringsMatchParse(t *testing.T) {
	for at := AnyAssetType; at <= RMSCloudAssetType; at++ {
		name := at.String()
		if name == "" {
			t.Errorf("AssetType %d has no string", at)
			continue
		}
		if at == AnyAssetType {
			// "Asset" is the zero value's name and is not parseable.
			continue
		}
		parsed, err := ParseAssetType(name)
		if err != nil {
			t.Errorf("ParseAssetType(%q) failed for AssetType %d: %s", name, at, err)
			continue
		}
		if parsed != at {
			t.Errorf("ParseAssetType(%q) = %d, want %d", name, parsed, at)
		}
	}
}
