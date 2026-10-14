package huawei

import (
	"strings"
)

// Label keys for the Enterprise Project a billed resource belongs to.
const (
	EnterpriseProjectIDLabel = "huawei_enterprise_project_id"
	EnterpriseProjectLabel   = "huawei_enterprise_project"
	// PlatformLabel carries the platform the resource's Enterprise Project
	// holds, as configured in CostConfiguration.EnterpriseProjects.
	PlatformLabel = "platform"
)

// defaultEnterpriseProjectID is the ID BSS reports for the "default"
// Enterprise Project. Confirmed against a real Huawei Cloud bill export, where
// MaaS token usage is billed to it.
const defaultEnterpriseProjectID = "0"

// enterpriseProjectDimension is the cost-analysed-bills group-by key for a
// resource's Enterprise Project. It follows the naming of the other dimension
// keys this integration uses (RESOURCE_ID, REGION_CODE).
const enterpriseProjectDimension = "ENTERPRISE_PROJECT_ID"

// resourceEnterpriseProjects records the Enterprise Project each resource was
// billed to, day by day. A resource can move between Enterprise Projects (a
// CCE node created before its pool had one, a manual migration), and BSS
// bills each day to the Enterprise Project the resource was in that day.
type resourceEnterpriseProjects struct {
	byDay  map[string]map[string]string // resource ID -> day -> Enterprise Project ID
	latest map[string]string            // resource ID -> Enterprise Project of its latest billed day
}

// lookup returns the Enterprise Project the resource was billed to on day,
// or, for a day with no record, the one of its latest billed day.
func (r resourceEnterpriseProjects) lookup(resourceID, day string) (string, bool) {
	if ep, ok := r.byDay[resourceID][day]; ok {
		return ep, true
	}
	ep, ok := r.latest[resourceID]
	return ep, ok
}

// fetchResourceEnterpriseProjects maps every resource billed in [beginTime,
// endTime] to its Enterprise Project ID, per day.
//
// It is a query of its own because BSS caps a cost query at three group-by
// dimensions and the main query (costQueryDimensions) already uses them.
// Grouping by resource and Enterprise Project returns one row per pair, with
// the days that pair was billed: that is enough to label every cost of the
// main query with the Enterprise Project of its own day.
func fetchResourceEnterpriseProjects(beginTime, endTime string) (resourceEnterpriseProjects, error) {
	projects := resourceEnterpriseProjects{byDay: map[string]map[string]string{}, latest: map[string]string{}}
	rows, err := fetchCostAnalysedBillsBy([]string{"RESOURCE_ID", enterpriseProjectDimension}, beginTime, endTime, "ORIGINAL_COST", "NET_AMOUNT")
	if err != nil {
		return projects, err
	}
	latestDay := map[string]string{}
	for _, row := range rows {
		resourceID := dimensionValue(row.Dimensions, "RESOURCE_ID")
		epID := strings.TrimSpace(dimensionValue(row.Dimensions, enterpriseProjectDimension))
		if resourceID == "" || epID == "" || epID == bssNullField {
			continue
		}
		id := describeResource(resourceID).ID
		if row.Costs == nil || len(*row.Costs) == 0 {
			if _, ok := projects.latest[id]; !ok {
				projects.latest[id] = epID
			}
			continue
		}
		for _, item := range *row.Costs {
			if item.TimeDimensionValue == nil || *item.TimeDimensionValue == "" {
				continue
			}
			day := *item.TimeDimensionValue
			if projects.byDay[id] == nil {
				projects.byDay[id] = map[string]string{}
			}
			projects.byDay[id][day] = epID
			if day >= latestDay[id] {
				latestDay[id] = day
				projects.latest[id] = epID
			}
		}
	}
	return projects, nil
}

// serviceCodeOf returns the service type code packed into a composite BSS
// RESOURCE_ID, or "" when the ID is not in that form.
func serviceCodeOf(resourceID string) string {
	fields := strings.Split(resourceID, ":")
	if len(fields) != bssResourceIDFields {
		return ""
	}
	return fields[0]
}
