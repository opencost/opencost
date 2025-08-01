package env

import (
	"github.com/opencost/opencost/core/pkg/env"
)

const (
	CloudCostConfigControllerStateFile = "cloud-configurations.json"
	CloudIntegrationConfigFile         = "cloud-integration.json"
	AzureBillingDataDownloadPath       = "db/cloudcost"
)

const (
	CloudCostEnabledEnvVar          = "CLOUD_COST_ENABLED"
	CloudCostMonthToDateIntervalVar = "CLOUD_COST_MONTH_TO_DATE_INTERVAL"
	CloudCostRefreshRateHoursEnvVar = "CLOUD_COST_REFRESH_RATE_HOURS"
	CloudCostQueryWindowDaysEnvVar  = "CLOUD_COST_QUERY_WINDOW_DAYS"
	CloudCostRunWindowDaysEnvVar    = "CLOUD_COST_RUN_WINDOW_DAYS"

	CustomCostEnabledEnvVar         = "CUSTOM_COST_ENABLED"
	CustomCostQueryWindowDaysEnvVar = "CUSTOM_COST_QUERY_WINDOW_DAYS"

	PluginConfigDirEnvVar     = "PLUGIN_CONFIG_DIR"
	PluginExecutableDirEnvVar = "PLUGIN_EXECUTABLE_DIR"

	AzureDownloadBillingDataToDiskEnvVar = "AZURE_DOWNLOAD_BILLING_DATA_TO_DISK"
)

func IsCloudCostEnabled() bool {
	return env.GetBool(CloudCostEnabledEnvVar, false)
}

func IsCustomCostEnabled() bool {
	return env.GetBool(CustomCostEnabledEnvVar, false)
}

func GetCloudCostConfigPath() string {
	return env.GetPathFromConfig(CloudIntegrationConfigFile)
}

func GetCloudCostMonthToDateInterval() int {
	return env.GetInt(CloudCostMonthToDateIntervalVar, 6)
}

func GetCloudCostRefreshRateHours() int64 {
	return env.GetInt64(CloudCostRefreshRateHoursEnvVar, 6)
}

func GetCloudCostQueryWindowDays() int64 {
	return env.GetInt64(CloudCostQueryWindowDaysEnvVar, 7)
}

func GetCustomCostQueryWindowHours() int64 {
	return env.GetInt64(CustomCostQueryWindowDaysEnvVar, 1)
}

func GetCustomCostQueryWindowDays() int64 {
	return env.GetInt64(CustomCostQueryWindowDaysEnvVar, 7)
}

func GetCloudCostRunWindowDays() int64 {
	return env.GetInt64(CloudCostRunWindowDaysEnvVar, 3)
}

func GetPluginConfigDir() string {
	return env.Get(PluginConfigDirEnvVar, "/opt/opencost/plugin/config")
}

func GetPluginExecutableDir() string {
	return env.Get(PluginExecutableDirEnvVar, "/opt/opencost/plugin/bin")
}

func GetAzureDownloadBillingDataPath() string {
	return env.GetPathFromConfig(AzureBillingDataDownloadPath)
}

func GetCloudCostConfigControllerStateFile() string {
	return env.GetPathFromConfig(CloudCostConfigControllerStateFile)
}
