package config

import (
	"sync"
)

//--------------------------------------------------------------------------
//  ConfigFileManager
//--------------------------------------------------------------------------

// ConfigFileManager is a fascade for a central API used to create and watch
// config files.
type ConfigFileManager struct {
	lock  *sync.Mutex
	files map[string]*ConfigFile
}

// NewConfigFileManager creates a new backing storage and configuration file manager
func NewConfigFileManager() *ConfigFileManager {
	return &ConfigFileManager{
		lock:  new(sync.Mutex),
		files: make(map[string]*ConfigFile),
	}
}

// ConfigFileAt returns an existing configuration file for the provided path if it exists. Otherwise,
// a new instance is created and returned. Note that the path does not have to exist in order for the
// instance to be created. It can exist as a potential file path on the storage, and be written to
// later
func (cfm *ConfigFileManager) ConfigFileAt(path string) *ConfigFile {
	cfm.lock.Lock()
	defer cfm.lock.Unlock()
	if cf, ok := cfm.files[path]; ok {
		return cf
	}

	cf := NewConfigFile(path)
	cfm.files[path] = cf
	return cf
}
