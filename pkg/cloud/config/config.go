package config

import (
	"fmt"
)

const Redacted = "REDACTED"

// Config allows for nested configurations which encapsulate their functionality to be validated and compared easily
type Config interface {
	Validate() error
	Sanitize() Config
	Equals(Config) bool
}

// KeyedConfig is a top level Config which uses its public values as a unique identifier allowing duplicates to be identified
type KeyedConfig interface {
	Config
	Key() string
}

type KeyedConfigWatcher interface {
	GetConfigs() []KeyedConfig
}

func GetInterfaceValue[T any](fmap map[string]interface{}, key string) (T, error) {
	var value T
	interfaceValue, ok := fmap[key]
	if !ok {
		return value, fmt.Errorf("FromInterface: missing '%s' property", key)
	}
	typedValue, ok := interfaceValue.(T)
	if !ok {
		return value, fmt.Errorf("GetInterfaceValue: property '%s' had expected type '%T' but did not match", key, value)
	}
	return typedValue, nil
}
