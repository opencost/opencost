package huawei

import (
	"sort"
	"strings"

	"github.com/opencost/opencost/core/pkg/log"
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
// billed to, day by day, with what each was billed. A resource can move between
// Enterprise Projects (a CCE node created before its pool had one, a manual
// migration), and BSS bills each day to the Enterprise Project the resource
// was in that day -- to more than one when it moved during the day.
type resourceEnterpriseProjects struct {
	byDay  map[string]map[string]map[string]float64 // resource ID -> day -> Enterprise Project ID -> net amount
	latest map[string]string                        // resource ID -> Enterprise Project of its latest billed day
}

// enterpriseProjectShare is the part of a resource's daily cost billed to
// one Enterprise Project.
type enterpriseProjectShare struct {
	id    string
	share float64
}

// lookup returns how the resource's cost on day splits across Enterprise
// Projects: in proportion to what each was billed that day, or, for a day
// with no record, all to the Enterprise Project of its latest billed day.
// It returns nil when the resource has no Enterprise Project at all.
func (r resourceEnterpriseProjects) lookup(resourceID, day string) []enterpriseProjectShare {
	amounts, ok := r.byDay[resourceID][day]
	if !ok || len(amounts) == 0 {
		if ep, ok := r.latest[resourceID]; ok {
			return []enterpriseProjectShare{{id: ep, share: 1}}
		}
		return nil
	}

	ids := make([]string, 0, len(amounts))
	total := 0.0
	for id, amount := range amounts {
		ids = append(ids, id)
		total += amount
	}
	sort.Strings(ids)

	shares := make([]enterpriseProjectShare, 0, len(ids))
	for _, id := range ids {
		share := 1 / float64(len(ids)) // nothing billed to weigh by: split evenly
		if total != 0 {
			share = amounts[id] / total
		}
		shares = append(shares, enterpriseProjectShare{id: id, share: share})
	}
	return shares
}

// fetchResourceEnterpriseProjects maps every resource billed in [beginTime,
// endTime] to its Enterprise Projects and what each was billed, per day.
//
// It is a query of its own because BSS caps a cost query at three group-by
// dimensions and the main query (costQueryDimensions) already uses them.
// Grouping by resource and Enterprise Project returns one row per pair, with
// the days that pair was billed and the amounts: that is enough to split every
// cost of the main query across the Enterprise Projects of its own day.
func fetchResourceEnterpriseProjects(beginTime, endTime string) (resourceEnterpriseProjects, error) {
	projects := resourceEnterpriseProjects{byDay: map[string]map[string]map[string]float64{}, latest: map[string]string{}}
	rows, err := fetchCostAnalysedBillsBy([]string{"RESOURCE_ID", enterpriseProjectDimension}, beginTime, endTime, "ORIGINAL_COST", "NET_AMOUNT")
	if err != nil {
		return projects, err
	}
	latestDay := map[string]string{}
	latestAmount := map[string]float64{}
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
			amount, err := parseCostAmount(item.Amount)
			if err != nil {
				log.Warnf("huawei cloud: Enterprise Project %s of %s on %s has an unparsable amount, counting it as 0: %v", epID, id, day, err)
				amount = 0
			}
			if projects.byDay[id] == nil {
				projects.byDay[id] = map[string]map[string]float64{}
			}
			if projects.byDay[id][day] == nil {
				projects.byDay[id][day] = map[string]float64{}
			}
			projects.byDay[id][day][epID] += amount

			// The latest day's Enterprise Project answers for days with no
			// record; when that day was split, the larger share does.
			if day > latestDay[id] || (day == latestDay[id] && projects.byDay[id][day][epID] > latestAmount[id]) {
				latestDay[id] = day
				latestAmount[id] = projects.byDay[id][day][epID]
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
