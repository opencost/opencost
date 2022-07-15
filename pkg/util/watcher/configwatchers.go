package watcher

import (
	"github.com/opencost/opencost/pkg/log"
	v1 "k8s.io/api/core/v1"
)

// ConfigMapWatcher represents a single configmap watcher
type ConfigMapWatcher struct {
	ConfigMapName string
	WatchFunc     func(string, map[string]string) error
}

type ConfigMapWatchers struct {
	watchers map[string][]*ConfigMapWatcher
}

func NewConfigMapWatchers(watchers ...*ConfigMapWatcher) *ConfigMapWatchers {
	cmw := &ConfigMapWatchers{
		watchers: make(map[string][]*ConfigMapWatcher),
	}

	for _, w := range watchers {
		cmw.AddWatcher(w)
	}

	return cmw
}

func (cmw *ConfigMapWatchers) AddWatcher(watcher *ConfigMapWatcher) {
	if watcher == nil {
		return
	}

	name := watcher.ConfigMapName
	cmw.watchers[name] = append(cmw.watchers[name], watcher)
}

func (cmw *ConfigMapWatchers) Add(configMapName string, watchFunc func(string, map[string]string) error) {
	cmw.AddWatcher(&ConfigMapWatcher{
		ConfigMapName: configMapName,
		WatchFunc:     watchFunc,
	})
}

func (cmw *ConfigMapWatchers) GetWatchedConfigs() []string {
	configNames := []string{}

	for k := range cmw.watchers {
		configNames = append(configNames, k)
	}

	return configNames
}

func (cmw *ConfigMapWatchers) ToWatchFunc() func(interface{}) {
	return func(c interface{}) {
		conf, ok := c.(*v1.ConfigMap)
		if !ok {
			return
		}

		name := conf.GetName()
		data := conf.Data
		if watchers, ok := cmw.watchers[name]; ok {
			for _, cw := range watchers {
				err := cw.WatchFunc(name, data)
				if err != nil {
					log.Infof("ERROR UPDATING %s CONFIG: %s", name, err.Error())
				}
			}
		}
	}
}
