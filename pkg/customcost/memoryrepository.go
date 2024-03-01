package customcost

import (
	"fmt"
	"sync"
	"time"

	"github.com/opencost/opencost/core/pkg/model"
	"golang.org/x/exp/maps"
)

// MemoryRepository is an implementation of Repository that uses a map keyed on config key and window start along with a
// RWMutex to make it threadsafe
type MemoryRepository struct {
	rwLock sync.RWMutex
	data   map[string]map[time.Time][]*model.CustomCostResponse
}

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{
		data: make(map[string]map[time.Time][]*model.CustomCostResponse),
	}
}

func (m *MemoryRepository) Has(startTime time.Time, domain string) (bool, error) {
	m.rwLock.RLock()
	defer m.rwLock.RUnlock()

	domainData, ok := m.data[domain]
	if !ok {
		return false, nil
	}

	_, ook := domainData[startTime.UTC()]
	return ook, nil
}

func (m *MemoryRepository) Get(startTime time.Time, domain string) ([]*model.CustomCostResponse, error) {
	m.rwLock.RLock()
	defer m.rwLock.RUnlock()

	domainData, ok := m.data[domain]
	if !ok {
		return nil, nil
	}

	ccr, ook := domainData[startTime.UTC()]
	if !ook {
		return nil, nil
	}
	clones := []*model.CustomCostResponse{}

	for _, cc := range ccr {
		clone := cc.Clone()
		clones = append(clones, &clone)
	}

	return clones, nil
}

func (m *MemoryRepository) Keys() ([]string, error) {
	m.rwLock.RLock()
	defer m.rwLock.RUnlock()

	keys := maps.Keys(m.data)
	return keys, nil
}

func (m *MemoryRepository) Put(ccr []*model.CustomCostResponse) error {
	m.rwLock.Lock()
	defer m.rwLock.Unlock()

	if ccr == nil {
		return fmt.Errorf("MemoryRepository: Put: cannot save nil")
	}

	for _, cc := range ccr {
		if cc.Window.IsOpen() {
			return fmt.Errorf("MemoryRepository: Put: custom cost response has invalid window %s", cc.Window.String())
		}

		if cc.GetDomain() == "" {
			return fmt.Errorf("MemoryRepository: Put: custom cost response does not have a domain value")
		}

		if _, ok := m.data[cc.GetDomain()]; !ok {
			m.data[cc.GetDomain()] = make(map[time.Time][]*model.CustomCostResponse)
		}

		m.data[cc.GetDomain()][cc.Window.Start().UTC()] = append(m.data[cc.GetDomain()][cc.Window.Start().UTC()], cc)
	}
	return nil
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
