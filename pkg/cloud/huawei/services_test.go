package huawei

import (
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/pkg/env"
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

// TestSelectHuaweiCategory_WiderCatalog covers the services added beyond the
// ones confirmed in a bill export, including MaaS token usage, which is billed
// under ModelArts.
func TestSelectHuaweiCategory_WiderCatalog(t *testing.T) {
	cases := []struct {
		service string
		want    string
	}{
		{"hws.service.type.modelarts", opencost.ComputeCategory},
		{"Cloud Search Service", opencost.StorageCategory},
		{"hws.service.type.css", opencost.StorageCategory},
		{"GaussDB(for MySQL)", opencost.StorageCategory},
		{"Enterprise Router", opencost.NetworkCategory},
		{"Virtual Private Network", opencost.NetworkCategory},
		{"Host Security Service", opencost.ManagementCategory},
		{"hws.service.type.hss", opencost.ManagementCategory},
		{"Cloud Trace Service", opencost.ManagementCategory},
		// Still unknown, still Other.
		{"Cloud Professional Services", opencost.OtherCategory},
	}
	for _, c := range cases {
		if got := selectHuaweiCategory(c.service); got != c.want {
			t.Errorf("selectHuaweiCategory(%q) = %q, want %q", c.service, got, c.want)
		}
	}
}

func TestLoadServiceCategories(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}

	loaded, err := loadServiceCategories(write("ok.json", `[
		{"category": "Compute", "codes": ["hws.service.type.MaaS"], "names": ["  Model as a Service "]},
		{"category": "Network", "codes": ["elb"]}
	]`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []service{
		{opencost.ComputeCategory, []string{"maas"}, []string{"model as a service"}},
		{opencost.NetworkCategory, []string{"elb"}, nil},
	}
	if !reflect.DeepEqual(loaded, want) {
		t.Fatalf("loaded %+v, want %+v", loaded, want)
	}

	for name, content := range map[string]string{
		"bad-category.json": `[{"category": "Security", "codes": ["hss"]}]`,
		"empty-entry.json":  `[{"category": "Compute"}]`,
		"not-json.json":     `{`,
	} {
		if _, err := loadServiceCategories(write(name, content)); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
	if _, err := loadServiceCategories(filepath.Join(dir, "missing.json")); err == nil {
		t.Errorf("expected an error for a missing file")
	}
}

// TestServiceTable_FileOverridesBuiltIn checks that file entries take
// precedence over the built-in table and add services it lacks.
func TestServiceTable_FileOverridesBuiltIn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "categories.json")
	content := `[
		{"category": "Shared", "codes": ["rds"]},
		{"category": "Compute", "codes": ["maas"], "names": ["model as a service"]}
	]`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(env.HuaweiServiceCategoriesEnvVar, path)

	serviceTableOnce = sync.Once{}
	defer func() {
		serviceTableOnce = sync.Once{}
		activeServiceTable = nil
	}()

	cases := map[string]string{
		"hws.service.type.rds":  opencost.SharedCategory,
		"hws.service.type.maas": opencost.ComputeCategory,
		"Model as a Service":    opencost.ComputeCategory,
		"hws.service.type.ec2":  opencost.ComputeCategory,
	}
	for service, want := range cases {
		if got := selectHuaweiCategory(service); got != want {
			t.Errorf("selectHuaweiCategory(%q) = %q, want %q", service, got, want)
		}
	}
}
