package config

import (
	"fmt"
	"sync"
	"time"

	"github.com/opencost/opencost/pkg/cloud"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/cloud/provider"
	"github.com/opencost/opencost/pkg/util/timeutil"
)

// configID identifies the source and the ID of a configuration to handle duplicate configs from multiple sources
type configID struct {
	source ConfigSource
	key    string
}

func (cid configID) Equals(that configID) bool {
	return cid.source == that.source && cid.key == that.key
}

func newConfigID(source, key string) configID {
	return configID{
		source: GetConfigSource(source),
		key:    key,
	}
}

type Status struct {
	Source ConfigSource
	Key    string
	Active bool
	Valid  bool
	Config cloud.KeyedConfig
}

// Controller manages the cloud.Config using config Watcher(s) to track various configuration
// methods. To do this it has a map of config watchers mapped on configuration source and a list Observers that it updates
// upon any change detected from the config watchers.
type Controller struct {
	statuses  map[configID]*Status
	observers []Observer
	watchers  map[ConfigSource]cloud.KeyedConfigWatcher
}

// NewController initializes an Config Controller
func NewController(cp models.Provider) *Controller {
	providerConfig := provider.ExtractConfigFromProviders(cp)
	watchers := GetCloudBillingWatchers(providerConfig)
	ic := &Controller{
		statuses: make(map[configID]*Status),
		watchers: watchers,
	}

	ic.load()
	ic.pullWatchers()

	go func() {
		ticker := timeutil.NewJobTicker()
		defer ticker.Close()

		for {
			ticker.TickIn(10 * time.Second)

			<-ticker.Ch

			ic.pullWatchers()
		}
	}()

	return ic
}

func (c *Controller) EnableConfig(key, source string) error {
	cID := newConfigID(source, key)
	cs, ok := c.statuses[cID]
	if !ok {
		return fmt.Errorf("Controller: EnableConfig: config with key %s from source %s does not exist", key, source)
	}
	if cs.Active {
		return fmt.Errorf("Controller: EnableConfig: config with key %s from source %s is already active", key, source)
	}

	// check for configurations with the same configuration key that are already active.
	for confID, confStat := range c.statuses {
		if confID.key != key || confID.source == cID.source {
			continue
		}

		// if active disable
		if confStat.Active == true {
			confStat.Active = false
		}
	}

	cs.Active = true
	c.putConfig(cs.Config)
	c.save()
	return nil
}

// DisableConfig updates an config status if it was enabled
func (c *Controller) DisableConfig(key, source string) error {
	iID := newConfigID(source, key)
	is, ok := c.statuses[iID]
	if !ok {
		return fmt.Errorf("Controller: DisableConfig: config with key %s from source %s does not exist", key, source)
	}
	if !is.Active {
		return fmt.Errorf("Controller: DisableConfig: config with key %s from source %s is already disabled", key, source)
	}

	is.Active = false
	c.deleteConfig(iID.key)
	c.save()
	return nil
}

// DeleteConfig removes an config from the statuses and deletes the config on all observers if it was active
func (c *Controller) DeleteConfig(key, source string) error {
	id := newConfigID(source, key)
	is, ok := c.statuses[id]
	if !ok {
		return fmt.Errorf("Controller: DisableConfig: config with key %s from source %s does not exist", key, source)
	}

	// delete config on observers if active
	if is.Active {
		c.deleteConfig(id.key)
	}
	delete(c.statuses, id)
	c.save()
	return nil
}

// pullWatchers retrieve configs from watchers and update configs according to priority of sources
func (c *Controller) pullWatchers() {

	for source, watcher := range c.watchers {
		for _, conf := range watcher.GetConfigs() {
			key := conf.Key()
			cID := configID{
				source: source,
				key:    key,
			}

			err := conf.Validate()
			valid := err == nil

			status := Status{
				Key:    key,
				Source: source,
				Active: valid, // active if valid, for now
				Valid:  valid,
				Config: conf,
			}

			// Check existing configs for matching key and source
			if existingStatus, ok := c.statuses[cID]; ok {
				// if config has not changed continue
				if existingStatus.Config.Equals(conf) {
					continue
				}
				// if existing CS is active then it should be replaced by the updated config
				if existingStatus.Active {
					if status.Valid {
						c.putConfig(conf)
					} else {
						// if active config is being overwritten by an invalid one, delete the config, as it will not be active
						c.deleteConfig(key)
					}
					c.statuses[cID] = &status
					continue
				}
			}

			// At this point we know that the config from this watcher has changed

			// handle an config with a new unique key for a source or an update config from a source which was inactive before
			if valid {
				for matchID, matchCS := range c.statuses {
					// skip matching configs
					if matchID.Equals(cID) {
						continue
					}

					if matchCS.Active {
						// if source is non-multi-cloud disable all other non-multi-cloud sourced configs
						if cID.source == HelmSource || cID.source == ConfigFileSource {
							if matchID.source == HelmSource || matchID.source == ConfigFileSource {
								matchCS.Active = false
								c.deleteConfig(matchID.key)
							}
						}

						// check for configs with the same key that are active
						if matchID.key == key {
							// If source has higher priority disable other active configs
							matchCS.Active = false
							c.deleteConfig(matchID.key)
						}
					}
				}
			}

			// update config and put to observers if active
			c.statuses[cID] = &status
			if status.Active {
				c.putConfig(conf)
			}
		}
	}
}

// todo implement when building config api and persistence is necessary
func (c *Controller) load() {}

// todo implement when building config api and persistence is necessary
func (c *Controller) save() {}

func (c *Controller) ExportConfigs(key string) (*Configurations, error) {
	configs := new(Configurations)

	activeConfigs := make(map[string]cloud.Config)
	for iID, cs := range c.statuses {
		if cs.Active {
			activeConfigs[iID.key] = cs.Config
		}
	}
	if key != "" {
		conf, ok := activeConfigs[key]
		if !ok {
			return nil, fmt.Errorf("Config with key %s does not exist or is inactive", key)
		}
		sanitizedConfig := conf.Sanitize()
		err := configs.Insert(sanitizedConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to insert config: %w", err)
		}
		return configs, nil
	}

	for _, conf := range activeConfigs {
		sanitizedConfig := conf.Sanitize()
		err := configs.Insert(sanitizedConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to insert config: %w", err)
		}
	}
	return configs, nil
}

func (c *Controller) getActiveConfigs() map[string]cloud.KeyedConfig {
	bi := make(map[string]cloud.KeyedConfig)
	for iID, cs := range c.statuses {
		if cs.Active {
			bi[iID.key] = cs.Config
		}
	}
	return bi
}

// deleteConfig ask observers to remove and stop all processes related to a configuration with a given key
func (c *Controller) deleteConfig(key string) {
	var wg sync.WaitGroup
	for _, obs := range c.observers {
		observer := obs
		wg.Add(1)
		go func() {
			defer wg.Done()
			observer.DeleteConfig(key)
		}()
	}
	wg.Wait()
}

// RegisterObserver gives out the current active list configs and adds the observer to the push list
func (c *Controller) RegisterObserver(obs Observer) {
	obs.SetConfigs(c.getActiveConfigs())
	c.observers = append(c.observers, obs)
}

func (c *Controller) GetStatus() []Status {
	var status []Status
	for _, intStat := range c.statuses {
		status = append(status, *intStat)
	}
	return status
}

// putConfig gives observers a new config to handle
func (c *Controller) putConfig(conf cloud.KeyedConfig) {
	var wg sync.WaitGroup
	for _, obs := range c.observers {
		observer := obs
		wg.Add(1)
		go func() {
			defer wg.Done()
			observer.PutConfig(conf)
		}()
	}
	wg.Wait()
}
