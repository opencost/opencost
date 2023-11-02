package config

import (
	"github.com/opencost/opencost/pkg/cloud"
)

// Observer should be implemented by any struct which need access to the up-to-date list of active configs
// that the Config.Controller provides. Any cloud billing Integration in the application that is used in the application
// should pass through this interface, and be revoked if it is not included in a Delete call.
type Observer interface {
	PutConfig(cloud.KeyedConfig)
	DeleteConfig(string)
	SetConfigs(map[string]cloud.KeyedConfig)
}
