package collector

import (
	"reflect"
	"testing"
	"time"

	"github.com/opencost/opencost/modules/collector-source/pkg/util"
)

func Test_repoStoreProvider_getStoreKeys(t *testing.T) {
	defaultResConfigs := []util.ResolutionConfiguration{
		{
			Interval: "10m",
		},
		{
			Interval: "1h",
		},
		{
			Interval: "1d",
		},
	}

	tests := map[string]struct {
		configs    []util.ResolutionConfiguration
		start      time.Time
		end        time.Time
		intevalKey string
		startKey   time.Time
	}{
		"10m": {
			configs:    defaultResConfigs,
			start:      time.Date(2025, time.May, 3, 0, 0, 0, 0, time.UTC),
			end:        time.Date(2025, time.May, 3, 0, 10, 0, 0, time.UTC),
			intevalKey: "10m",
			startKey:   time.Date(2025, time.May, 3, 0, 0, 0, 0, time.UTC),
		},
		"1h": {
			configs:    defaultResConfigs,
			start:      time.Date(2025, time.May, 3, 0, 0, 0, 0, time.UTC),
			end:        time.Date(2025, time.May, 3, 1, 0, 0, 0, time.UTC),
			intevalKey: "1h",
			startKey:   time.Date(2025, time.May, 3, 0, 0, 0, 0, time.UTC),
		},
		"1d": {
			configs:    defaultResConfigs,
			start:      time.Date(2025, time.May, 3, 0, 0, 0, 0, time.UTC),
			end:        time.Date(2025, time.May, 4, 0, 10, 0, 0, time.UTC),
			intevalKey: "1d",
			startKey:   time.Date(2025, time.May, 3, 0, 0, 0, 0, time.UTC),
		},
		"2m": {
			configs:    defaultResConfigs,
			start:      time.Date(2025, time.May, 3, 0, 0, 0, 0, time.UTC),
			end:        time.Date(2025, time.May, 3, 0, 2, 0, 0, time.UTC),
			intevalKey: "10m",
			startKey:   time.Date(2025, time.May, 3, 0, 0, 0, 0, time.UTC),
		},
		"2m offset": {
			configs:    defaultResConfigs,
			start:      time.Date(2025, time.May, 3, 0, 9, 0, 0, time.UTC),
			end:        time.Date(2025, time.May, 3, 0, 11, 0, 0, time.UTC),
			intevalKey: "10m",
			startKey:   time.Date(2025, time.May, 3, 0, 0, 0, 0, time.UTC),
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			r := newRepoStoreProvider(nil, tt.configs)
			intevalKey, startKey := r.getStoreKeys(tt.start, tt.end)
			if intevalKey != tt.intevalKey {
				t.Errorf("getStoreKeys() got = %v, want %v", intevalKey, tt.intevalKey)
			}
			if !reflect.DeepEqual(startKey, tt.startKey) {
				t.Errorf("getStoreKeys() got1 = %v, want %v", startKey, tt.startKey)
			}
		})
	}
}
