package huawei

import (
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/pkg/env"
)

// serviceCategoryEntry is one entry of the HUAWEICLOUD_SERVICE_CATEGORIES file,
// e.g.
//
//	[{"category": "Storage", "codes": ["css"], "names": ["cloud search service"]}]
//
// Codes are Service Type Code suffixes (after "hws.service.type."); names are
// English display names. Both are matched case-insensitively.
type serviceCategoryEntry struct {
	Category string   `json:"category"`
	Codes    []string `json:"codes"`
	Names    []string `json:"names"`
}

var validServiceCategories = []string{
	opencost.ComputeCategory,
	opencost.StorageCategory,
	opencost.NetworkCategory,
	opencost.ManagementCategory,
	opencost.SharedCategory,
	opencost.OtherCategory,
}

var (
	serviceTableOnce   sync.Once
	activeServiceTable []service
)

// serviceTable returns the services lookupService matches against: the
// entries of the HUAWEICLOUD_SERVICE_CATEGORIES file, if set, ahead of the
// built-in table, so that a deployment can correct a category or add a
// service the built-in table lacks without a rebuild. A file that cannot be
// read is logged and ignored; the built-in table still applies.
func serviceTable() []service {
	serviceTableOnce.Do(func() {
		activeServiceTable = services
		path := env.GetHuaweiServiceCategoriesPath()
		if path == "" {
			return
		}
		extra, err := loadServiceCategories(path)
		if err != nil {
			log.Errorf("huawei cloud: ignoring %s=%s: %v", env.HuaweiServiceCategoriesEnvVar, path, err)
			return
		}
		activeServiceTable = append(extra, services...)
		log.Infof("huawei cloud: loaded %d service categories from %s", len(extra), path)
	})
	return activeServiceTable
}

func loadServiceCategories(path string) ([]service, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var entries []serviceCategoryEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("parsing: %w", err)
	}

	loaded := make([]service, 0, len(entries))
	for i, entry := range entries {
		if !slices.Contains(validServiceCategories, entry.Category) {
			return nil, fmt.Errorf("entry %d: category %q is not one of %v", i, entry.Category, validServiceCategories)
		}
		svc := service{category: entry.Category}
		for _, code := range entry.Codes {
			code = strings.TrimPrefix(strings.ToLower(strings.TrimSpace(code)), serviceTypeCodePrefix)
			if code != "" {
				svc.codes = append(svc.codes, code)
			}
		}
		for _, name := range entry.Names {
			if name = strings.ToLower(strings.TrimSpace(name)); name != "" {
				svc.names = append(svc.names, name)
			}
		}
		if len(svc.codes) == 0 && len(svc.names) == 0 {
			return nil, fmt.Errorf("entry %d: needs at least one code or name", i)
		}
		loaded = append(loaded, svc)
	}
	return loaded, nil
}
