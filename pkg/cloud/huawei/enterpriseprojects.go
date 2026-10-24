package huawei

import (
	"fmt"
	"sort"
	"strings"

	"github.com/shopspring/decimal"

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
	byDay  map[string]map[string]map[string]*billedAmounts // resource ID -> day -> Enterprise Project ID -> amounts
	latest map[string]string                               // resource ID -> Enterprise Project of its latest billed day
}

// billedAmounts is what BSS billed one Enterprise Project for a resource on
// one day: the net amount and the list (official) amount. They are kept as
// decimals, as BSS reports them, so that amounts which cancel out sum to
// exactly zero (see allocate): in floating point, 0.10 + 0.20 - 0.30 does not.
type billedAmounts struct {
	net, list decimal.Decimal
}

// enterpriseProjectCost is the part of a resource's daily cost that belongs
// to one Enterprise Project.
type enterpriseProjectCost struct {
	id        string
	net, list float64
}

// split divides a resource's daily net and list cost across the Enterprise
// Projects it was billed to that day, each in proportion to what each was
// billed, or, for a day with no record, gives it all to the Enterprise
// Project of its latest billed day. It returns nil when the resource has no
// Enterprise Project at all.
func (r resourceEnterpriseProjects) split(resourceID, day string, net, list float64) []enterpriseProjectCost {
	billed, ok := r.byDay[resourceID][day]
	if !ok || len(billed) == 0 {
		if ep, ok := r.latest[resourceID]; ok {
			return []enterpriseProjectCost{{id: ep, net: net, list: list}}
		}
		return nil
	}

	ids := make([]string, 0, len(billed))
	for id := range billed {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	byNet := func(id string) decimal.Decimal { return billed[id].net }
	byList := func(id string) decimal.Decimal { return billed[id].list }
	nets := allocate(net, ids, byNet, byList)
	lists := allocate(list, ids, byList, byNet)
	costs := make([]enterpriseProjectCost, 0, len(ids))
	for i, id := range ids {
		costs = append(costs, enterpriseProjectCost{id: id, net: nets[i], list: lists[i]})
	}
	return costs
}

// allocate divides total across ids in proportion to what each was billed.
//
// When the billed amounts sum to zero there is no proportion to use. If some
// are nonzero -- a charge in one Enterprise Project and an equal adjustment
// in another -- each keeps its own signed amount, and whatever of total they
// do not account for is split evenly. If all are zero, the amounts carry no
// information (BSS left them out), so the other amount's proportions are
// used instead (fallback), and an even split only when it has none either.
func allocate(total float64, ids []string, billed, fallback func(string) decimal.Decimal) []float64 {
	sum, allZero := decimal.Zero, true
	for _, id := range ids {
		sum = sum.Add(billed(id))
		allZero = allZero && billed(id).IsZero()
	}
	if allZero && fallback != nil {
		return allocate(total, ids, fallback, nil)
	}
	parts := make([]float64, len(ids))
	for i, id := range ids {
		amount := billed(id).InexactFloat64()
		if !sum.IsZero() {
			parts[i] = total * amount / sum.InexactFloat64()
		} else {
			parts[i] = amount + total/float64(len(ids))
		}
	}
	return parts
}

// parseCostDecimal parses a BSS amount exactly; a missing one is zero, and an
// unparsable one is zero with an error.
func parseCostDecimal(s *string) (decimal.Decimal, error) {
	if s == nil || strings.TrimSpace(*s) == "" {
		return decimal.Zero, nil
	}
	d, err := decimal.NewFromString(strings.TrimSpace(*s))
	if err != nil {
		return decimal.Zero, fmt.Errorf("unable to parse amount %q: %w", *s, err)
	}
	return d, nil
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
	projects := resourceEnterpriseProjects{byDay: map[string]map[string]map[string]*billedAmounts{}, latest: map[string]string{}}
	rows, err := fetchCostAnalysedBillsBy([]string{"RESOURCE_ID", enterpriseProjectDimension}, beginTime, endTime, "ORIGINAL_COST", "NET_AMOUNT")
	if err != nil {
		return projects, err
	}
	latestDay := map[string]string{}
	latestAmount := map[string]decimal.Decimal{}
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
			net, err := parseCostDecimal(item.Amount)
			if err != nil {
				log.Warnf("huawei cloud: Enterprise Project %s of %s on %s has an unparsable amount, counting it as 0: %v", epID, id, day, err)
			}
			list, err := parseCostDecimal(item.OfficialAmount)
			if err != nil {
				log.Warnf("huawei cloud: Enterprise Project %s of %s on %s has an unparsable official amount, counting it as 0: %v", epID, id, day, err)
			}
			if projects.byDay[id] == nil {
				projects.byDay[id] = map[string]map[string]*billedAmounts{}
			}
			if projects.byDay[id][day] == nil {
				projects.byDay[id][day] = map[string]*billedAmounts{}
			}
			amounts := projects.byDay[id][day][epID]
			if amounts == nil {
				amounts = &billedAmounts{}
				projects.byDay[id][day][epID] = amounts
			}
			amounts.net = amounts.net.Add(net)
			amounts.list = amounts.list.Add(list)

			// The latest day's Enterprise Project answers for days with no
			// record; when that day was split, the larger share does.
			if day > latestDay[id] || (day == latestDay[id] && amounts.net.GreaterThan(latestAmount[id])) {
				latestDay[id] = day
				latestAmount[id] = amounts.net
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
