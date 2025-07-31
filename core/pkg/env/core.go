package env

import (
	"path"
)

const DefaultRootPath = "/var/configs"
const DefaultStorageFile = "federated-store.yaml"

const (
	APIPortEnvVar   = "API_PORT"
	ClusterIDEnvVar = "CLUSTER_ID"
	RootPathEnvVar  = "CONFIG_PATH"

	PProfEnabledEnvVar = "PPROF_ENABLED"
)

// GetAPIPort returns the environment variable value for APIPortEnvVar which
// is the port number the API is available on.
func GetAPIPortWithDefault(def int) int {
	return GetInt(APIPortEnvVar, def)
}

// GetClusterID returns the environment variable value for ClusterIDEnvVar which represents the
// configurable identifier used for multi-cluster metric emission.
func GetClusterID() string {
	return Get(ClusterIDEnvVar, "")
}

// GetConfigPath returns the environment variable value for ConfigPathEnvVar which represents the cost
// model configuration path
func GetRootPath() string {
	return Get(RootPathEnvVar, DefaultRootPath)
}

func GetPathFromRoot(subPath string) string {
	return path.Join(GetRootPath(), subPath)
}

func GetDefaultStorageConfigFilePath() string {
	return path.Join(GetRootPath(), DefaultStorageFile)
}

func IsPProfEnabled() bool {
	return GetBool(PProfEnabledEnvVar, false)
}
