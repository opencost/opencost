package env

import (
	"path"
)

const DefaultConfigPath = "/var/configs"
const DefaultStorageFile = "federated-store.yaml"

const (
	APIPortEnvVar    = "API_PORT"
	ClusterIDEnvVar  = "CLUSTER_ID"
	ConfigPathEnvVar = "CONFIG_PATH"

	PProfEnabledEnvVar = "PPROF_ENABLED"

	InstallNamespaceEnvVar = "INSTALL_NAMESPACE"
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
func GetConfigPath() string {
	return Get(ConfigPathEnvVar, DefaultConfigPath)
}

func GetPathFromConfig(subPaths ...string) string {
	subPath := path.Join(subPaths...)
	return path.Join(GetConfigPath(), subPath)
}

func GetDefaultStorageConfigFilePath() string {
	return path.Join(GetConfigPath(), DefaultStorageFile)
}

func IsPProfEnabled() bool {
	return GetBool(PProfEnabledEnvVar, false)
}

func GetInstallNamespace(def string) string {
	return Get(InstallNamespaceEnvVar, def)
}
