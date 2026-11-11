package cloudcost

import (
	"fmt"
	"sync"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	"golang.org/x/exp/maps"
)

// MemoryRepository is an implementation of Repository that uses a map keyed on config key and window start along with a
// RWMutex to make it threadsafe
type MemoryRepository struct {
	rwLock sync.RWMutex
	data   map[string]map[time.Time]*opencost.CloudCostSet
}

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{
		data: make(map[string]map[time.Time]*opencost.CloudCostSet),
	}
}

func (m *MemoryRepository) Has(startTime time.Time, billingIntegration string) (bool, error) {
	m.rwLock.RLock()
	defer m.rwLock.RUnlock()

	billingIntegrationData, ok := m.data[billingIntegration]
	if !ok {
		return false, nil
	}

	_, ook := billingIntegrationData[startTime.UTC()]
	return ook, nil
}

// Get returns the stored CloudCostSet for the given window start and billing
// integration, or nil if none is stored. The returned set is shared with the
// repository and must be treated as read-only; stored sets are never mutated
// after Put, so they are safe to read concurrently. Returning the shared set
// avoids deep-copying every entry of a resource-level billing day on each
// query, which previously dominated the transient memory of the query path.
func (m *MemoryRepository) Get(startTime time.Time, billingIntegration string) (*opencost.CloudCostSet, error) {
	m.rwLock.RLock()
	defer m.rwLock.RUnlock()

	billingIntegrationData, ok := m.data[billingIntegration]
	if !ok {
		return nil, nil
	}

	ccs, ook := billingIntegrationData[startTime.UTC()]
	if !ook {
		return nil, nil
	}
	return ccs, nil
}

func (m *MemoryRepository) Keys() ([]string, error) {
	m.rwLock.RLock()
	defer m.rwLock.RUnlock()

	keys := maps.Keys(m.data)
	return keys, nil
}

func (m *MemoryRepository) Put(ccs *opencost.CloudCostSet) error {
	m.rwLock.Lock()
	defer m.rwLock.Unlock()

	if ccs == nil {
		return fmt.Errorf("MemoryRepository: Put: cannot save nil")
	}

	if ccs.Window.IsOpen() {
		return fmt.Errorf("MemoryRepository: Put: cloud cost set has invalid window %s", ccs.Window.String())
	}

	if ccs.Integration == "" {
		return fmt.Errorf("MemoryRepository: Put: cloud cost set does not have an integration value")
	}

	if _, ok := m.data[ccs.Integration]; !ok {
		m.data[ccs.Integration] = make(map[time.Time]*opencost.CloudCostSet)
	}

	integrationData := m.data[ccs.Integration]
	startTime := ccs.Window.Start().UTC()
	shareProperties(ccs, integrationData, startTime)
	integrationData[startTime] = ccs
	return nil
}

// shareProperties replaces the CloudCostProperties of each entry in the given
// set with the equal instance already stored under the same key in the set
// being replaced or in an adjacent window, when one exists. Billing data is
// dominated by resources that persist across days and by windows that are
// re-ingested on every refresh, so most entries carry properties (strings and
// label maps) identical to ones the repository already holds; sharing a single
// instance collapses that duplication and substantially reduces the resident
// memory of the repository. This is safe because stored sets are never mutated
// after Put: aggregation replaces Properties pointers on its own copies and
// never writes through them.
func shareProperties(ccs *opencost.CloudCostSet, integrationData map[time.Time]*opencost.CloudCostSet, startTime time.Time) {
	duration := ccs.Window.Duration()
	if duration <= 0 {
		return
	}

	// Candidate sets that may already hold equal properties: the set being
	// replaced (re-ingested window), the previous window, and the next window
	// (the initial backfill ingests newest-first).
	var priors []*opencost.CloudCostSet
	for _, candidateStart := range []time.Time{startTime, startTime.Add(-duration), startTime.Add(duration)} {
		if prior, ok := integrationData[candidateStart]; ok && prior != nil {
			priors = append(priors, prior)
		}
	}
	if len(priors) == 0 {
		return
	}

	for key, cc := range ccs.CloudCosts {
		if cc == nil || cc.Properties == nil {
			continue
		}
		for _, prior := range priors {
			priorCC, ok := prior.CloudCosts[key]
			if !ok || priorCC == nil || priorCC.Properties == nil {
				continue
			}
			if cc.Properties.Equal(priorCC.Properties) {
				cc.Properties = priorCC.Properties
				break
			}
		}
	}
}

// Expire deletes all items in the map with a start time before the given limit
func (m *MemoryRepository) Expire(limit time.Time) error {
	m.rwLock.Lock()
	defer m.rwLock.Unlock()

	for key, integration := range m.data {
		for startTime := range integration {
			if startTime.Before(limit) {
				delete(integration, startTime)
			}
		}
		// remove integration if it is now empty
		if len(integration) == 0 {
			delete(m.data, key)
		}
	}
	return nil
}
