package cloud

import (
	"fmt"
	gopath "path"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/kubecost/cost-model/pkg/config"
	"github.com/kubecost/cost-model/pkg/env"
	"github.com/kubecost/cost-model/pkg/log"
	"github.com/kubecost/cost-model/pkg/util/json"
	"github.com/microcosm-cc/bluemonday"
)

var sanitizePolicy = bluemonday.UGCPolicy()

// ProviderConfig is a utility class that provides a thread-safe configuration storage/cache for all Provider
// implementations
type ProviderConfig struct {
	lock            *sync.Mutex
	configManager   *config.ConfigFileManager
	configFile      *config.ConfigFile
	customPricing   *CustomPricing
	watcherHandleID config.HandlerID
}

// NewProviderConfig creates a new ConfigFile and returns the ProviderConfig
func NewProviderConfig(configManager *config.ConfigFileManager, fileName string) *ProviderConfig {
	configFile := configManager.ConfigFileAt(configPathFor(fileName))
	pc := &ProviderConfig{
		lock:          new(sync.Mutex),
		configManager: configManager,
		configFile:    configFile,
		customPricing: nil,
	}

	// add the provider config func as handler for the config file changes
	pc.watcherHandleID = configFile.AddChangeHandler(pc.onConfigFileUpdated)
	return pc
}

// onConfigFileUpdated handles any time the config file contents are updated, created, or deleted
func (pc *ProviderConfig) onConfigFileUpdated(changeType config.ChangeType, data []byte) {
	// TODO: (bolt) Currently this has the side-effect of setting pc.customPricing twice when the update
	// TODO: (bolt) is made from this ProviderConfig instance. We'll need to implement a way of identifying
	// TODO: (bolt) when to ignore updates when the change and handler are the same source
	log.Infof("CustomPricing Config Updated: %s", changeType)

	switch changeType {
	case config.ChangeTypeCreated:
		fallthrough
	case config.ChangeTypeModified:
		pc.lock.Lock()
		defer pc.lock.Unlock()

		customPricing := new(CustomPricing)
		err := json.Unmarshal(data, customPricing)
		if err != nil {
			log.Infof("Could not decode Custom Pricing file at path %s. Using default.", pc.configFile.Path())
			customPricing = DefaultPricing()
		}

		pc.customPricing = customPricing
		if pc.customPricing.SpotGPU == "" {
			pc.customPricing.SpotGPU = DefaultPricing().SpotGPU // Migration for users without this value set by default.
		}

		if pc.customPricing.ShareTenancyCosts == "" {
			pc.customPricing.ShareTenancyCosts = defaultShareTenancyCost
		}
	}
}

// Non-ThreadSafe logic to load the config file if a cache does not exist. Flag to write
// the default config if the config file doesn't exist.
func (pc *ProviderConfig) loadConfig(writeIfNotExists bool) (*CustomPricing, error) {
	if pc.customPricing != nil {
		return pc.customPricing, nil
	}

	exists, err := pc.configFile.Exists()
	// File Error other than NotExists
	if err != nil {
		log.Infof("Custom Pricing file at path '%s' read error: '%s'", pc.configFile.Path(), err.Error())
		return DefaultPricing(), err
	}

	// File Doesn't Exist
	if !exists {
		log.Infof("Could not find Custom Pricing file at path '%s'", pc.configFile.Path())
		pc.customPricing = DefaultPricing()

		// Only write the file if flag enabled
		if writeIfNotExists {
			cj, err := json.Marshal(pc.customPricing)
			if err != nil {
				return pc.customPricing, err
			}

			err = pc.configFile.Write(cj)
			if err != nil {
				log.Infof("Could not write Custom Pricing file to path '%s'", pc.configFile.Path())
				return pc.customPricing, err
			}
		}

		return pc.customPricing, nil
	}

	// File Exists - Read all contents of file, unmarshal json
	byteValue, err := pc.configFile.Read()
	if err != nil {
		log.Infof("Could not read Custom Pricing file at path %s", pc.configFile.Path())
		// If read fails, we don't want to cache default, assuming that the file is valid
		return DefaultPricing(), err
	}

	var customPricing CustomPricing
	err = json.Unmarshal(byteValue, &customPricing)
	if err != nil {
		log.Infof("Could not decode Custom Pricing file at path %s", pc.configFile.Path())
		return DefaultPricing(), err
	}

	pc.customPricing = &customPricing
	if pc.customPricing.SpotGPU == "" {
		pc.customPricing.SpotGPU = DefaultPricing().SpotGPU // Migration for users without this value set by default.
	}

	if pc.customPricing.ShareTenancyCosts == "" {
		pc.customPricing.ShareTenancyCosts = defaultShareTenancyCost
	}

	return pc.customPricing, nil
}

// ThreadSafe method for retrieving the custom pricing config.
func (pc *ProviderConfig) GetCustomPricingData() (*CustomPricing, error) {
	pc.lock.Lock()
	defer pc.lock.Unlock()

	return pc.loadConfig(true)
}

// ConfigFileManager returns the ConfigFileManager instance used to manage the CustomPricing
// configuration. In the event of a multi-provider setup, this instance should be used to
// configure any other configuration providers.
func (pc *ProviderConfig) ConfigFileManager() *config.ConfigFileManager {
	return pc.configManager
}

// Allows a call to manually update the configuration while maintaining proper thread-safety
// for read/write methods.
func (pc *ProviderConfig) Update(updateFunc func(*CustomPricing) error) (*CustomPricing, error) {
	pc.lock.Lock()
	defer pc.lock.Unlock()

	// Load Config, set flag to _not_ write if failure to find file.
	// We're about to write the updated values, so we don't want to double write.
	c, _ := pc.loadConfig(false)

	// Execute Update - On error, return the in-memory config but don't update cache
	// explicitly
	err := updateFunc(c)
	if err != nil {
		return c, err
	}

	// Cache Update (possible the ptr already references the cached value)
	pc.customPricing = c

	cj, err := json.Marshal(c)
	if err != nil {
		return c, err
	}
	err = pc.configFile.Write(cj)

	if err != nil {
		return c, err
	}

	return c, nil
}

// ThreadSafe update of the config using a string map
func (pc *ProviderConfig) UpdateFromMap(a map[string]string) (*CustomPricing, error) {
	// Run our Update() method using SetCustomPricingField logic
	return pc.Update(func(c *CustomPricing) error {
		for k, v := range a {
			// Just so we consistently supply / receive the same values, uppercase the first letter.
			kUpper := strings.Title(k)
			if kUpper == "CPU" || kUpper == "SpotCPU" || kUpper == "RAM" || kUpper == "SpotRAM" || kUpper == "GPU" || kUpper == "Storage" {
				val, err := strconv.ParseFloat(v, 64)
				if err != nil {
					return fmt.Errorf("Unable to parse CPU from string to float: %s", err.Error())
				}
				v = fmt.Sprintf("%f", val/730)
			}

			err := SetCustomPricingField(c, kUpper, v)
			if err != nil {
				return err
			}
		}

		return nil
	})
}

// DefaultPricing should be returned so we can do computation even if no file is supplied.
func DefaultPricing() *CustomPricing {
	return &CustomPricing{
		Provider:              "base",
		Description:           "Default prices based on GCP us-central1",
		CPU:                   "0.031611",
		SpotCPU:               "0.006655",
		RAM:                   "0.004237",
		SpotRAM:               "0.000892",
		GPU:                   "0.95",
		SpotGPU:               "0.308",
		Storage:               "0.00005479452",
		ZoneNetworkEgress:     "0.01",
		RegionNetworkEgress:   "0.01",
		InternetNetworkEgress: "0.12",
		CustomPricesEnabled:   "false",
		ShareTenancyCosts:     "true",
	}
}

func SetCustomPricingField(obj *CustomPricing, name string, value string) error {

	structValue := reflect.ValueOf(obj).Elem()
	structFieldValue := structValue.FieldByName(name)

	if !structFieldValue.IsValid() {
		return fmt.Errorf("No such field: %s in obj", name)
	}

	if !structFieldValue.CanSet() {
		return fmt.Errorf("Cannot set %s field value", name)
	}

	structFieldType := structFieldValue.Type()
	value = sanitizePolicy.Sanitize(value)
	val := reflect.ValueOf(value)
	if structFieldType != val.Type() {
		return fmt.Errorf("Provided value type didn't match custom pricing field type")
	}

	structFieldValue.Set(val)
	return nil
}

// Returns the configuration directory concatenated with a specific config file name
func configPathFor(filename string) string {
	path := env.GetConfigPathWithDefault("/models/")
	return gopath.Join(path, filename)
}
