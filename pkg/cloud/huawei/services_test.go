package huawei

import (
	"os"
	"path/filepath"
	"reflect"
	"slices"
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

// TestServiceTable_OverrideCoversCodesAndNames checks that a file entry
// naming a service by only one form applies to every form BSS may report it
// in, so the category does not depend on whether the bill carries the code or
// the English name -- and that it stays with that service.
func TestServiceTable_OverrideCoversCodesAndNames(t *testing.T) {
	path := filepath.Join(t.TempDir(), "categories.json")
	content := `[
		{"category": "Shared", "codes": ["rds"]},
		{"category": "Other", "names": ["elastic cloud server"]}
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
		// Code-only override, reached by code, bare code and name.
		"hws.service.type.rds":        opencost.SharedCategory,
		"RDS":                         opencost.SharedCategory,
		"Relational Database Service": opencost.SharedCategory,
		// Name-only override, reached by name and by either alias code.
		"Elastic Cloud Server": opencost.OtherCategory,
		"hws.service.type.ec2": opencost.OtherCategory,
		"hws.service.type.ecs": opencost.OtherCategory,
		// Other services in the same built-in category are unaffected.
		"Bare Metal Server":         opencost.ComputeCategory,
		"hws.service.type.gaussdb":  opencost.StorageCategory,
		"Document Database Service": opencost.StorageCategory,
	}
	for service, want := range cases {
		if got := selectHuaweiCategory(service); got != want {
			t.Errorf("selectHuaweiCategory(%q) = %q, want %q", service, got, want)
		}
	}
}

// TestServiceTypeCode_KeepsIdentityUnderGroupingOverride checks that a
// HUAWEICLOUD_SERVICE_CATEGORIES entry grouping services under one category
// changes their category but not their identity: Bare Metal Server stays BMS
// in both forms BSS may report it, and a service only the file knows still
// resolves to its code.
func TestServiceTypeCode_KeepsIdentityUnderGroupingOverride(t *testing.T) {
	path := filepath.Join(t.TempDir(), "categories.json")
	content := `[
		{"category": "Shared", "codes": ["ec2", "bms"]},
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
		"Bare Metal Server":    "hws.service.type.bms",
		"hws.service.type.bms": "hws.service.type.bms",
		"Elastic Cloud Server": "hws.service.type.ec2",
		"Model as a Service":   "hws.service.type.maas",
	}
	for service, want := range cases {
		if got := serviceTypeCode(service); got != want {
			t.Errorf("serviceTypeCode(%q) = %q, want %q", service, got, want)
		}
	}
	for _, service := range []string{"Bare Metal Server", "hws.service.type.bms", "Elastic Cloud Server"} {
		if got := selectHuaweiCategory(service); got != opencost.SharedCategory {
			t.Errorf("selectHuaweiCategory(%q) = %q, want the override's Shared", service, got)
		}
	}
	if got := serviceDisplayName(serviceTypeCode("Bare Metal Server")); got != "BMS" {
		t.Errorf("expected Bare Metal Server to be reported as BMS, got %q", got)
	}
}

// TestSelectHuaweiCategory_LongestQualifiedNameWins covers a qualified name
// that contains the names of two services: with an SFS-only override listed
// ahead of the built-in table, "Scalable File Service Turbo (pay-per-use)"
// must still resolve to SFS Turbo, not to SFS through its shorter name.
func TestSelectHuaweiCategory_LongestQualifiedNameWins(t *testing.T) {
	path := filepath.Join(t.TempDir(), "categories.json")
	if err := os.WriteFile(path, []byte(`[{"category": "Shared", "codes": ["sfs"]}]`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(env.HuaweiServiceCategoriesEnvVar, path)
	serviceTableOnce = sync.Once{}
	defer func() {
		serviceTableOnce = sync.Once{}
		activeServiceTable = nil
	}()

	cases := map[string]string{
		"Scalable File Service Turbo (pay-per-use)": opencost.StorageCategory,
		"Scalable File Service Turbo":               opencost.StorageCategory,
		"hws.service.type.sfsturbo":                 opencost.StorageCategory,
		"Scalable File Service (pay-per-use)":       opencost.SharedCategory,
		"Scalable File Service":                     opencost.SharedCategory,
		"hws.service.type.sfs":                      opencost.SharedCategory,
	}
	for service, want := range cases {
		if got := selectHuaweiCategory(service); got != want {
			t.Errorf("selectHuaweiCategory(%q) = %q, want %q", service, got, want)
		}
	}
	if got := serviceTypeCode("Scalable File Service Turbo (pay-per-use)"); got != "hws.service.type.sfsturbo" {
		t.Errorf("expected SFS Turbo's code, got %q", got)
	}
}

// TestServices_OneServicePerEntry guards the table's invariant that an entry
// is a single service: no code or name may appear in two entries, and the
// services that used to share an entry with another one stand alone.
func TestServices_OneServicePerEntry(t *testing.T) {
	seen := map[string]int{}
	for i, svc := range services {
		for _, id := range append(slices.Clone(svc.codes), svc.names...) {
			if j, dup := seen[id]; dup && j != i {
				t.Errorf("%q appears in entries %d and %d", id, j, i)
			}
			seen[id] = i
		}
	}
	for _, pair := range [][2]string{{"bms", "ec2"}, {"sfsturbo", "sfs"}, {"csms", "kms"}, {"cse", "servicestage"}, {"ccm", "scm"}, {"aad", "antiddos"}, {"cnad", "antiddos"}, {"taurusdb", "gaussdb"}, {"nosql", "gaussdb"}} {
		if canonicalServiceCode(pair[0]) == canonicalServiceCode(pair[1]) {
			t.Errorf("%q and %q must be distinct services", pair[0], pair[1])
		}
	}
}
