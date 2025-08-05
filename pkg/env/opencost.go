package env

import (
	"time"

	"github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

// Environment variables specific to the running of opencost
const (
	DefaultAPIPort           = 9003
	defaultOpencostNamespace = "opencost"
	DefaultConfigMountPath   = "/var/configs"
)

const (
	UTCOffsetEnvVar = "UTC_OFFSET"
	ConfigPathEnvVar = "CONFIG_PATH"
)

func GetOpencostAPIPort() int {
	return env.GetAPIPortWithDefault(DefaultAPIPort)
}

// GetOpencostNamespace returns the environment variable value that is set for the kubernetes namespace
// this service is installed in.
func GetOpencostNamespace() string {
	return env.GetInstallNamespace(defaultOpencostNamespace)
}

// GetUTCOffset returns the environment variable value for UTCOffset
func GetUTCOffset() string {
	return env.Get(UTCOffsetEnvVar, "")
}

// GetParsedUTCOffset returns the duration of the configured UTC offset
func GetParsedUTCOffset() time.Duration {
	offset, err := timeutil.ParseUTCOffset(GetUTCOffset())
	if err != nil {
		log.Warnf("Failed to parse UTC offset: %s", err)
		return time.Duration(0)
	}
	return offset
}

// GetConfigPathWithDefault returns the environment variable value for ConfigPathEnvVar if set,
// otherwise returns the provided default path
func GetConfigPathWithDefault(defaultPath string) string {
	return env.GetConfigPathWithDefault(defaultPath)
}

// GetClusterID returns the environment variable value for ClusterIDEnvVar which represents the
// configurable identifier used for multi-cluster metric emission.
func GetClusterID() string {
	return env.GetClusterID()
}

// GetInstallNamespace returns the environment variable value for InstallNamespaceEnvVar
func GetInstallNamespace() string {
	return env.GetInstallNamespace(defaultOpencostNamespace)
}
