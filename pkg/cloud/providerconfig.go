package cloud

import (
	"fmt"
	"io/ioutil"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/kubecost/cost-model/pkg/env"
	"github.com/kubecost/cost-model/pkg/util"
	"github.com/kubecost/cost-model/pkg/util/json"
	"github.com/microcosm-cc/bluemonday"

	"k8s.io/klog"
)

var sanitizePolicy = bluemonday.UGCPolicy()

// ProviderConfig is a utility class that provides a thread-safe configuration
// storage/cache for all Provider implementations
type ProviderConfig struct {
	lock          *sync.Mutex
	fileName      string
	configPath    string
	customPricing *CustomPricing
}

// Creates a new ProviderConfig instance
func NewProviderConfig(file string) *ProviderConfig {
	return &ProviderConfig{
		lock:          new(sync.Mutex),
		fileName:      file,
		configPath:    configPathFor(file),
		customPricing: nil,
	}
}

// Non-ThreadSafe logic to load the config file if a cache does not exist. Flag to write
// the default config if the config file doesn't exist.
func (pc *ProviderConfig) loadConfig(writeIfNotExists bool) (*CustomPricing, error) {
	if pc.customPricing != nil {
		return pc.customPricing, nil
	}

	exists, err := fileExists(pc.configPath)
	// File Error other than NotExists
	if err != nil {
		klog.Infof("Custom Pricing file at path '%s' read error: '%s'", pc.configPath, err.Error())
		return DefaultPricing(), err
	}

	// File Doesn't Exist
	if !exists {
		klog.Infof("Could not find Custom Pricing file at path '%s'", pc.configPath)
		pc.customPricing = DefaultPricing()

		// Only write the file if flag enabled
		if writeIfNotExists {
			cj, err := json.Marshal(pc.customPricing)
			if err != nil {
				return pc.customPricing, err
			}

			err = ioutil.WriteFile(pc.configPath, cj, 0644)
			if err != nil {
				klog.Infof("Could not write Custom Pricing file to path '%s'", pc.configPath)
				return pc.customPricing, err
			}
		}

		return pc.customPricing, nil
	}

	// File Exists - Read all contents of file, unmarshal json
	byteValue, err := ioutil.ReadFile(pc.configPath)
	if err != nil {
		klog.Infof("Could not read Custom Pricing file at path %s", pc.configPath)
		// If read fails, we don't want to cache default, assuming that the file is valid
		return DefaultPricing(), err
	}

	var customPricing CustomPricing
	err = json.Unmarshal(byteValue, &customPricing)
	if err != nil {
		klog.Infof("Could not decode Custom Pricing file at path %s", pc.configPath)
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
	err = ioutil.WriteFile(pc.configPath, cj, 0644)

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

	// shared overhead costs go into SharedCosts with key "total" in CustomPricing
	if name == "SharedOverhead" {

		value = sanitizePolicy.Sanitize(value)
		val := reflect.ValueOf(value)

		if reflect.ValueOf(obj.SharedCosts["total"]).Type() != val.Type() {
			return fmt.Errorf("cannot insert value into custom pricing shared costs")
		}

		obj.SharedCosts["total"] = value

		return nil

	}

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

// File exists has three different return cases that should be handled:
//   1. File exists and is not a directory (true, nil)
//   2. File does not exist (false, nil)
//   3. File may or may not exist. Error occurred during stat (false, error)
// The third case represents the scenario where the stat returns an error,
// but the error isn't relevant to the path. This can happen when the current
// user doesn't have permission to access the file.
func fileExists(filename string) (bool, error) {
	return util.FileExists(filename) // delegate to utility method
}

// Returns the configuration directory concatenated with a specific config file name
func configPathFor(filename string) string {
	path := env.GetConfigPathWithDefault("/models/")
	return path + filename
}
