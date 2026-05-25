package configrbac

import (
	"os"
	"sync"

	"github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/json"
)

const configFileName = "config.json"

// AppConfig is the root structure for CONFIG_PATH/config.json.
type AppConfig struct {
	RBAC RBACConfig `json:"rbac"`
}

// RBACConfig holds RBAC-related feature flags and settings.
type RBACConfig struct {
	ScopedViews ScopedViewsConfig `json:"scopedViews"`
}

// ScopedViewsConfig toggles the scoped views persistence API.
type ScopedViewsConfig struct {
	Enabled bool `json:"enabled"`
}

// ConfigLoader reads config.json from the OpenCost config directory.
type ConfigLoader struct {
	path string
	mu   sync.RWMutex
}

// NewConfigLoader returns a loader for CONFIG_PATH/config.json.
func NewConfigLoader() *ConfigLoader {
	return &ConfigLoader{path: env.GetPathFromConfig(configFileName)}
}

// ConfigPath returns the resolved config.json path.
func (l *ConfigLoader) ConfigPath() string {
	return l.path
}

// Load reads config.json. A missing file or disabled flag yields Enabled=false.
func (l *ConfigLoader) Load() (AppConfig, error) {
	l.mu.RLock()
	path := l.path
	l.mu.RUnlock()

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return AppConfig{}, nil
		}
		return AppConfig{}, err
	}

	var cfg AppConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		log.Warnf("configrbac: invalid %s: %v", path, err)
		return AppConfig{}, nil
	}
	return cfg, nil
}

// ScopedViewsEnabled reports whether rbac.scopedViews.enabled is true in config.json.
func (l *ConfigLoader) ScopedViewsEnabled() (bool, error) {
	cfg, err := l.Load()
	if err != nil {
		return false, err
	}
	return cfg.RBAC.ScopedViews.Enabled, nil
}
